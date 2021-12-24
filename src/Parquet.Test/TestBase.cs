using System;
using System.IO;
using Parquet.Data;
using System.Linq;
using F = System.IO.File;
using Parquet.Data.Rows;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Parquet.Test
{
   public class TestBase
   {
      protected Stream OpenTestFile(string name)
      {
         return F.OpenRead("./data/" + name);
      }

      /*protected T[] ConvertSerialiseDeserialise<T>(IEnumerable<T> instances) where T: new()
      {
         using (var ms = new MemoryStream())
         {
            Schema s = ParquetConvert.Serialize<T>(instances, ms);

            ms.Position = 0;

            return ParquetConvert.Deserialize<T>(ms);
         }
      }*/

      protected async Task<Table> ReadTestFileAsTable(string name)
      {
         using (Stream s = OpenTestFile(name))
         {
            using (ParquetReader reader = await ParquetReader.Open(s))
            {
               return await reader.ReadAsTable();
            }
         }
      }

      protected async Task<Table> WriteRead(Table table, bool saveLocal = false)
      {
         var ms = new MemoryStream();

         using (ParquetWriter writer = await ParquetWriter.Open(table.Schema, ms))
         {
            await writer.Write(table);
         }

         if(saveLocal)
         {
            F.WriteAllBytes("c:\\tmp\\test.parquet", ms.ToArray());
         }

         ms.Position = 0;

         using (ParquetReader reader = await ParquetReader.Open(ms))
         {
            return await reader.ReadAsTable();
         }
      }

      protected async Task<DataColumn> WriteReadSingleColumn(DataField field, DataColumn dataColumn)
      {
         using (var ms = new MemoryStream())
         {
            // write with built-in extension method
            await ms.WriteSingleRowGroupParquetFile(new Schema(field), dataColumn);
            ms.Position = 0;

            //System.IO.File.WriteAllBytes("c:\\tmp\\1.parquet", ms.ToArray());

            // read first gow group and first column
            using (ParquetReader reader = await ParquetReader.Open(ms))
            {
               if (reader.RowGroupCount == 0) return null;
               ParquetRowGroupReader rgReader = reader.OpenRowGroupReader(0);

               return await rgReader.ReadColumn(field);
            }


         }
      }

      protected async Task<(DataColumn[], Schema)> WriteReadSingleRowGroup(Schema schema, DataColumn[] columns)
      {
         using (var ms = new MemoryStream())
         {
            await ms.WriteSingleRowGroupParquetFile(schema, columns);
            ms.Position = 0;

            //System.IO.File.WriteAllBytes("c:\\tmp\\1.parquet", ms.ToArray());

            using (ParquetReader reader = await ParquetReader.Open(ms))
            {
               DataColumn[] e1;
               Schema readSchema = reader.Schema;

               using (ParquetRowGroupReader rgReader = reader.OpenRowGroupReader(0))
               {
                  e1 = await Task.WhenAll(columns.Select(c =>
                     rgReader.ReadColumn(c.Field)));
               }

               return (e1, readSchema);
            }
         }
      }

      protected async Task<object> WriteReadSingle(DataField field, object value, CompressionMethod compressionMethod = CompressionMethod.None, int compressionLevel = -1)
      {
         //for sanity, use disconnected streams
         byte[] data;

         using (var ms = new MemoryStream())
         {
            // write single value

            using (ParquetWriter writer = await ParquetWriter.Open(new Schema(field), ms))
            {
               writer.CompressionMethod = compressionMethod;
               writer.CompressionLevel = compressionLevel;

               using (ParquetRowGroupWriter rg = writer.CreateRowGroup())
               {
                  Array dataArray = Array.CreateInstance(field.ClrNullableIfHasNullsType, 1);
                  dataArray.SetValue(value, 0);
                  var column = new DataColumn(field, dataArray);

                  await rg.WriteColumn(column);
               }
            }

            data = ms.ToArray();
         }

         using (var ms = new MemoryStream(data))
         { 
            // read back single value

            ms.Position = 0;
            using (ParquetReader reader = await ParquetReader.Open(ms))
            {
               using (ParquetRowGroupReader rowGroupReader = reader.OpenRowGroupReader(0))
               {
                  DataColumn column = await rowGroupReader.ReadColumn(field);

                  return column.Data.GetValue(0);
               }
            }
         }
      }
   }
}