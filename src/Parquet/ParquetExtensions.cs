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
         using (ParquetWriter writer = await ParquetWriter.Open(schema, stream))
         {
            writer.CompressionMethod = CompressionMethod.None;
            using (ParquetRowGroupWriter rgw = writer.CreateRowGroup())
            {
               foreach (DataColumn column in columns)
               {
                  await rgw.WriteColumn(column);
               }
            }
         }
      }

      /// <summary>
      /// Writes entire table in a single row group
      /// </summary>
      /// <param name="writer"></param>
      /// <param name="table"></param>
      public static async Task Write(this ParquetWriter writer, Table table)
      {
         using (ParquetRowGroupWriter rowGroupWriter = writer.CreateRowGroup())
         {
            await rowGroupWriter.Write(table);
         }
      }

      /// <summary>
      /// Reads the first row group as a table
      /// </summary>
      /// <param name="reader">Open reader</param>
      /// <returns></returns>
      public static async Task<Table> ReadAsTable(this ParquetReader reader)
      {
         throw new NotImplementedException();
         /*
         Table result = null;

         for (int i = 0; i < reader.RowGroupCount; i++)
         {
            using (ParquetRowGroupReader rowGroupReader = reader.OpenRowGroupReader(i))
            {
               DataColumn[] allData = await Task.WhenAll(reader.Schema.GetDataFields().Select(df => rowGroupReader.ReadColumn(df)));

               var t = new Table(reader.Schema, allData, rowGroupReader.RowCount);

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
         }

         return result;
         */
      }

      /// <summary>
      /// Writes table to this row group
      /// </summary>
      /// <param name="writer"></param>
      /// <param name="table"></param>
      public static async Task Write(this ParquetRowGroupWriter writer, Table table)
      {
         foreach (DataColumn dc in table.ExtractDataColumns())
         {
            await writer.WriteColumn(dc);
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

         IDataTypeHandler handler = DataTypeFactory.Match(schema, new ParquetOptions { TreatByteArrayAsString = true });

         using (var ms = new MemoryStream(rawBytes))
         using (var reader = new BinaryReader(ms))
         {
            object value = handler.Read(reader, schema, rawBytes.Length);
            return value;
         }
      }
   }
}