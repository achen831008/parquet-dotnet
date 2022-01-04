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
   /// Represents row group reading and writing functionality.
   /// </summary>
   public class RowGroup
   {
      internal readonly Thrift.RowGroup _rowGroup;
      private readonly Options _options;
      private readonly Stream _s;
      private readonly ThriftFooter _footer;
      private readonly Dictionary<string, Thrift.ColumnChunk> _pathToChunk = new Dictionary<string, Thrift.ColumnChunk>();

      internal RowGroup(Thrift.RowGroup rowGroup,
         Options options,
         Stream s,
         ThriftFooter footer)
      {
         _rowGroup = rowGroup;
         _options = options;
         _s = s;
         _footer = footer;

         //cache chunks
         if (rowGroup.Columns != null)
         {
            foreach (Thrift.ColumnChunk thriftChunk in rowGroup.Columns)
            {
               string path = thriftChunk.GetPath();
               _pathToChunk[path] = thriftChunk;
            }
         }
      }

      /// <summary>
      /// Gets the number of rows in this row group
      /// </summary>
      public long RowCount => _rowGroup.Num_rows;

      /// <summary>
      /// Reads column data
      /// </summary>
      /// <param name="field"></param>
      /// <returns></returns>
      /// <exception cref="NotImplementedException"></exception>
      public async Task<DataColumn> ReadAsync(DataField field)
      {
         if (!_pathToChunk.TryGetValue(field.Path, out Thrift.ColumnChunk columnChunk))
            throw new ParquetException($"'{field.Path}' does not exist in this file");

         var columnReader = new DataColumnReader(field, _s, columnChunk, _footer, _options);

         return await columnReader.Read();
      }

      /// <summary>
      /// Reads all the column data from this row group.
      /// </summary>
      /// <remarks>Not recommended to use performance wise. Always choose what column(s) to read specifically.</remarks>
      /// <returns></returns>
      public async Task<DataColumn[]> ReadAllColumnsAsync()
      {
         DataField[] allFields = _footer.CreateModelSchema(_options).GetDataFields();
         var result = new DataColumn[allFields.Length];

         for(int i = 0; i < result.Length; i++)
         {
            DataColumn column = await ReadAsync(allFields[i]);
            result[i] = column;
         }

         return result;
      }
   }
}
