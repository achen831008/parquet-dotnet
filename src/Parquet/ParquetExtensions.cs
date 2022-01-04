using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Parquet.Data;
using Parquet.Data.Rows;
using Parquet.File;

namespace Parquet
{
   /// <summary>
   /// Defines extension methods to simplify Parquet usage (experimental v3)
   /// </summary>
   public static class ParquetExtensions
   {
      /// <summary>
      /// Writes a file with a single row group
      /// </summary>
      public static async Task WriteSingleRowGroupParquetFile(this Stream stream, Schema schema, params DataColumn[] columns)
      {
         await using (ParquetFile writer = await ParquetFile.CreateAsync(stream, schema))
         {
            using (WriteableRowGroup rgw = writer.NewRowGroup())
            {
               foreach (DataColumn column in columns)
               {
                  await rgw.WriteAsync(column);
               }
            }
         }
      }

      /// <summary>
      /// Writes entire table in a single row group
      /// </summary>
      /// <param name="pf"></param>
      /// <param name="table"></param>
      public static async Task Write(this ParquetFile pf, Table table)
      {
         using (WriteableRowGroup rg = pf.NewRowGroup())
         {
            await rg.Write(table);
         }
      }

      /// <summary>
      /// Reads the first row group as a table
      /// </summary>
      /// <param name="pf">Open reader</param>
      /// <returns></returns>
      public static async Task<Table> ReadAsTable(this ParquetFile pf)
      {
         Table result = null;

         foreach (RowGroup rg in pf.RowGroups)
         {
            DataColumn[] allData = await rg.ReadAllColumnsAsync();

            var t = new Table(pf.Schema, allData, rg.RowCount);

            if (result == null)
            {
               result = t;
            }
            else
            {
               foreach (Row row in t)
               {
                  result.Add(row);
               }
            }
         }

         return result;
      }

      /// <summary>
      /// Writes table to this row group
      /// </summary>
      /// <param name="writer"></param>
      /// <param name="table"></param>
      public static async Task Write(this WriteableRowGroup writer, Table table)
      {
         foreach (DataColumn dc in table.ExtractDataColumns())
         {
            await writer.WriteAsync(dc);
         }
      }

      /// <summary>
      /// Decodes raw bytes from <see cref="Thrift.Statistics"/> into a CLR value
      /// </summary>
      internal static object DecodeSingleStatsValue(this Thrift.FileMetaData fileMeta, Thrift.ColumnChunk columnChunk, byte[] rawBytes)
      {
         if (rawBytes == null || rawBytes.Length == 0) return null;

         var footer = new ThriftFooter(fileMeta);
         Thrift.SchemaElement schema = footer.GetSchemaElement(columnChunk);

         IDataTypeHandler handler = DataTypeFactory.Match(schema, new Options { TreatByteArrayAsString = true });

         using (var ms = new MemoryStream(rawBytes))
         using (var reader = new BinaryReader(ms))
         {
            object value = handler.Read(reader, schema, rawBytes.Length);
            return value;
         }
      }
   }
}