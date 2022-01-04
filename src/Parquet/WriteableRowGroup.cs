using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Parquet.Data;
using Parquet.File;

namespace Parquet
{
   /// <summary>
   /// 
   /// </summary>
   public class WriteableRowGroup : RowGroup, IDisposable
   {
      private readonly ParquetFile _pf;
      private readonly Thrift.SchemaElement[] _thschema;
      private int _nextColIdx;

      internal WriteableRowGroup(ParquetFile pf) 
         : base(
              pf.Footer.AddRowGroup(),
              pf.Options,
              pf.NativeStream,
              pf.Footer)
      {
         _pf = pf;
         _thschema = pf.Footer.GetWriteableSchema();
      }

      /// <summary>
      /// Writes next data column to parquet stream. Note that columns must be written in the order they are declared in the
      /// file schema.
      /// </summary>
      /// <param name="column"></param>
      public async Task WriteAsync(DataColumn column)
      {
         if (column == null) throw new ArgumentNullException(nameof(column));

         if (_rowGroup.Num_rows == 0)
         {
            if (column.Data.Length > 0 || column.Field.MaxRepetitionLevel == 0)
               _rowGroup.Num_rows = column.CalculateRowCount();
         }

         Thrift.SchemaElement tse = _thschema[_nextColIdx];
         if (!column.Field.Equals(tse))
         {
            throw new ArgumentException(
               $"cannot write this column, expected '{tse.Name}', passed: '{column.Field.Name}'",
               nameof(column));
         }
         IDataTypeHandler dataTypeHandler = DataTypeFactory.Match(tse, _pf.Options);
         _nextColIdx += 1;

         List<string> path = _pf.Footer.GetPath(tse);

         var writer = new DataColumnWriter(
            _pf.NativeStream,
            _pf.ThriftStream,
            _pf.Footer, tse,
            Thrift.CompressionCodec.UNCOMPRESSED,
            -1,
            (int)_rowGroup.Num_rows);

         Thrift.ColumnChunk chunk = await writer.Write(path, column, dataTypeHandler);
         _rowGroup.Columns.Add(chunk);
      }

      /// <summary>
      /// Commits row group to parent file
      /// </summary>
      /// <returns></returns>
      /// <exception cref="NotImplementedException"></exception>
      public void Dispose()
      {
         //row group's size is a sum of _uncompressed_ sizes of all columns in it, including the headers
         //luckily ColumnChunk already contains sizes of page+header in it's meta
         _rowGroup.Total_byte_size = _rowGroup.Columns.Sum(c => c.Meta_data.Total_uncompressed_size);
         _rowGroup.Total_compressed_size = _rowGroup.Columns.Sum(c => c.Meta_data.Total_compressed_size);

         _pf.Commit(this);
      }
   }
}
