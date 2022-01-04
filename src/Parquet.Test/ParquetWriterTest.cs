using System;
using Parquet.Data;
using System.IO;
using Xunit;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Linq;

namespace Parquet.Test
{
   public class ParquetWriterTest : TestBase
   {
      [Fact]
      public async Task Can_write_simplest_file()
      {
         var id = new DataField<int>("id");
         var name = new DataField<string>("name");
         var schema = new Schema(id, name);
         var ms = new MemoryStream();

         await using(ParquetFile pf = await ParquetFile.CreateAsync(ms, schema))
         {
            using (WriteableRowGroup rg = pf.NewRowGroup())
            {
               await rg.WriteAsync(new DataColumn(id, new[] { 1, 2, 3}));
               await rg.WriteAsync(new DataColumn(name, new[] { "one", "two", "three" }));
            }
         }

         // read the file and validate data
         ms.Position = 0;
         await using(ParquetFile pf = await ParquetFile.OpenAsync(ms))
         {
            Assert.Single(pf.RowGroups);

            DataColumn[] cols = await pf.RowGroups.First().ReadAllColumnsAsync();
            Assert.Equal(2, cols.Length);
         }
      }

      [Fact]
      public async Task Cannot_write_columns_in_wrong_order()
      {
         var schema = new Schema(new DataField<int>("id"), new DataField<int>("id2"));

         //await using ParquetFile pf = await ParquetFile.CreateAsync(new MemoryStream(), schema);

         await using (ParquetFile writer = await ParquetFile.CreateAsync(new MemoryStream(), schema))
         {
            using (WriteableRowGroup gw = writer.NewRowGroup())
            {
               await Assert.ThrowsAsync<ArgumentException>(() =>
                  gw.WriteAsync(new DataColumn((DataField)schema[1], new int[] { 1 })));
            }
         }
      }

      [Fact]
      public async Task Write_in_small_row_groups()
      {
         //write a single file having 3 row groups
         var id = new DataField<int>("id");
         var ms = new MemoryStream();

         await using (ParquetFile pf = await ParquetFile.CreateAsync(ms, new Schema(id)))
         {
            using (WriteableRowGroup rg = pf.NewRowGroup())
            {
               await rg.WriteAsync(new DataColumn(id, new int[] { 1 }));
            }

            using (WriteableRowGroup rg = pf.NewRowGroup())
            {
               await rg.WriteAsync(new DataColumn(id, new int[] { 2 }));
            }

            using (WriteableRowGroup rg = pf.NewRowGroup())
            {
               await rg.WriteAsync(new DataColumn(id, new int[] { 3 }));
            }

         }

         //read the file back and validate
         ms.Position = 0;
         await using (ParquetFile reader = await ParquetFile.OpenAsync(ms))
         {
            Assert.Equal(3, reader.RowGroups.Count);

            RowGroup rg = reader.RowGroups.First();
            Assert.Equal(1, rg.RowCount);
            DataColumn dc = await rg.ReadAsync(id);
            Assert.Equal(new int[] { 1 }, dc.Data);

            rg = reader.RowGroups.ElementAt(1);
            Assert.Equal(1, rg.RowCount);
            dc = await rg.ReadAsync(id);
            Assert.Equal(new int[] { 2 }, dc.Data);

            rg = reader.RowGroups.ElementAt(2);
            Assert.Equal(1, rg.RowCount);
            dc = await rg.ReadAsync(id);
            Assert.Equal(new int[] { 3 }, dc.Data);
         }
      }

      [Fact]
      public async Task Append_to_file_reads_all_data()
      {
         //write a file with a single row group
         var id = new DataField<int>("id");
         var ms = new MemoryStream();

         await using (ParquetFile writer = await ParquetFile.CreateAsync(ms, new Schema(id)))
         {
            using (WriteableRowGroup rg = writer.NewRowGroup())
            {
               await rg.WriteAsync(new DataColumn(id, new int[] { 1, 2 }));
            }
         }

         //append to this file. Note that you cannot append to existing row group, therefore create a new one
         ms.Position = 0;
         await using (ParquetFile writer = await ParquetFile.CreateAsync(ms, new Schema(id)))
         {
            using (WriteableRowGroup rg = writer.NewRowGroup())
            {
               await rg.WriteAsync(new DataColumn(id, new int[] { 3, 4 }));
            }
         }

         //check that this file now contains two row groups and all the data is valid
         ms.Position = 0;
         await using (ParquetFile reader = await ParquetFile.OpenAsync(ms))
         {
            Assert.Equal(2, reader.RowGroups.Count);

            RowGroup rg = reader.RowGroups.ElementAt(0);
            Assert.Equal(2, rg.RowCount);
            Assert.Equal(new int[] { 1, 2 }, (await rg.ReadAsync(id)).Data);

            rg = reader.RowGroups.ElementAt(1);
            Assert.Equal(2, rg.RowCount);
            Assert.Equal(new int[] { 3, 4 }, (await rg.ReadAsync(id)).Data);
         }
      }
      
      public readonly static IEnumerable<object[]> NullableColumnContentCases = new List<object[]>()
      {
         new object[] { new int?[] { 1, 2 } },
         new object[] { new int?[] { null } },
         new object[] { new int?[] { 1, null, 2 } },
         new object[] { new int[] { 1, 2 } },
      };

      [Theory]
      [MemberData(nameof(NullableColumnContentCases))]
      public async Task Write_read_nullable_column(Array input)
      {
         var id = new DataField<int?>("id");
         var ms = new MemoryStream();

         await using (ParquetFile writer = await ParquetFile.CreateAsync(ms, new Schema(id)))
         {
            using (WriteableRowGroup rg = writer.NewRowGroup())
            {
               await rg.WriteAsync(new DataColumn(id, input));
            }
         }

         ms.Position = 0;
         await using (ParquetFile reader = await ParquetFile.OpenAsync(ms))
         {
            Assert.Equal(1, reader.RowGroups.Count);

            RowGroup rg = reader.RowGroups.First();
            Assert.Equal(input.Length, rg.RowCount);
            Assert.Equal(input, (await rg.ReadAsync(id)).Data);
         }
      }

      [Fact]
      public async Task FileMetadata_sets_num_rows_on_file_and_row_group()
      {
         var ms = new MemoryStream();
         var id = new DataField<int>("id");

         //write
         await using (ParquetFile writer = await ParquetFile.CreateAsync(ms, new Schema(id)))
         {
            using (WriteableRowGroup rg = writer.NewRowGroup())
            {
               await rg.WriteAsync(new DataColumn(id, new[] { 1, 2, 3, 4 }));
            }
         }

         //read back
         await using (ParquetFile reader = await ParquetFile.OpenAsync(ms))
         {
            Assert.Equal(4, reader.TotalRowCount);

            Assert.Equal(4, reader.RowGroups.First().RowCount);
         }
      }

      [Fact]
      public async Task FileMetadata_sets_num_rows_on_file_and_row_group_multiple_row_groups()
      {
         var ms = new MemoryStream();
         var id = new DataField<int>("id");

         //write
         await using (ParquetFile writer = await ParquetFile.CreateAsync(ms, new Schema(id)))
         {
            using (WriteableRowGroup rg = writer.NewRowGroup())
            {
               await rg.WriteAsync(new DataColumn(id, new[] { 1, 2, 3, 4 }));
            }

            using (WriteableRowGroup rg = writer.NewRowGroup())
            {
               await rg.WriteAsync(new DataColumn(id, new[] { 5, 6 }));
            }
         }

         //read back
         await using (ParquetFile reader = await ParquetFile.OpenAsync(ms))
         {
            Assert.Equal(6, reader.TotalRowCount);

            Assert.Equal(4, reader.RowGroups.ElementAt(0).RowCount);
            Assert.Equal(2, reader.RowGroups.ElementAt(1).RowCount);
         }
      }

      [Fact]
      public async Task CustomMetadata_can_write_and_read()
      {
         var ms = new MemoryStream();
         var id = new DataField<int>("id");

         //write
         await using (ParquetFile writer = await ParquetFile.CreateAsync(ms, new Schema(id)))
         {
            writer.Metadata["key1"] = "value1";
            writer.Metadata["key2"] = "value2";

            using (WriteableRowGroup rg = writer.NewRowGroup())
            {
               await rg.WriteAsync(new DataColumn(id, new[] { 1, 2, 3, 4 }));
            }
         }

         //read back
         await using (ParquetFile reader = await ParquetFile.OpenAsync(ms))
         {
            Assert.Equal("value1", reader.Metadata["key1"]);
            Assert.Equal("value2", reader.Metadata["key2"]);
         }
      }
   }
}