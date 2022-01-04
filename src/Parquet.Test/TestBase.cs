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
         using Stream s = OpenTestFile(name);
         await using ParquetFile pf = await ParquetFile.OpenAsync(s);
         return await pf.ReadAsTable();
      }

      protected async Task<Table> WriteRead(Table table, bool saveLocal = false)
      {
         var ms = new MemoryStream();

         await using (ParquetFile writer = await ParquetFile.CreateAsync(ms, table.Schema))
         {
            await writer.Write(table);
         }

         if(saveLocal)
         {
            F.WriteAllBytes("c:\\tmp\\test.parquet", ms.ToArray());
         }

         ms.Position = 0;

         await using (ParquetFile reader = await ParquetFile.OpenAsync(ms))
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
            await using (ParquetFile reader = await ParquetFile.OpenAsync(ms))
            {
               if (reader.RowGroups.Count == 0) return null;
               return await reader.RowGroups.First().ReadAsync(field);
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

            await using (ParquetFile reader = await ParquetFile.OpenAsync(ms))
            {
               Schema readSchema = reader.Schema;

               DataColumn[] e1 = await reader.RowGroups.First().ReadAllColumnsAsync();

               return (e1, readSchema);
            }
         }
      }

      protected async Task<object> WriteReadSingle(
         DataField field,
         object value,
         Thrift.CompressionCodec compressionMethod = Thrift.CompressionCodec.UNCOMPRESSED)
      {
         //for sanity, use disconnected streams
         byte[] data;

         using (var ms = new MemoryStream())
         {
            // write single value

            await using (ParquetFile writer = await ParquetFile.CreateAsync(ms, new Schema(field)))
            {
               using (WriteableRowGroup rg = writer.NewRowGroup())
               {
                  Array dataArray = Array.CreateInstance(field.ClrNullableIfHasNullsType, 1);
                  dataArray.SetValue(value, 0);
                  var column = new DataColumn(field, dataArray);

                  await rg.WriteAsync(column);
               }
            }

            data = ms.ToArray();
         }

         using (var ms = new MemoryStream(data))
         { 
            // read back single value

            ms.Position = 0;
            await using (ParquetFile reader = await ParquetFile.OpenAsync(ms))
            {
               DataColumn column = await reader.RowGroups.First().ReadAsync(field);
               return column.Data.GetValue(0);
            }
         }
      }
   }
}