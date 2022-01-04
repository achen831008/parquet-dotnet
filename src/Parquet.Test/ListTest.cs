using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Parquet.Data;
using Xunit;

namespace Parquet.Test
{
   public class ListTest : TestBase
   {
      [Fact]
      public async Task List_of_structures_writes_reads()
      {
         var idsch = new DataField<int>("id");
         var cnamech = new DataField<string>("name");
         var ccountrych = new DataField<string>("country");

         var schema = new Schema(
            idsch,
            new ListField("cities",
            new StructField("element",
               cnamech,
               ccountrych)));

         var id = new DataColumn(idsch, new int[] { 1 });
         var cname = new DataColumn(cnamech, new[] { "London", "New York" }, new[] { 0, 1 });
         var ccountry = new DataColumn(ccountrych, new[] { "UK", "US" }, new[] { 0, 1 });

         (DataColumn[] readColumns, Schema readSchema) = await WriteReadSingleRowGroup(schema, new[] { id, cname, ccountry });
      }

      [Fact]
      public async Task List_of_elements_with_some_items_empty_reads_file()
      {
         /*
          list data:
          - 1: [1, 2, 3]
          - 2: []
          - 3: [1, 2, 3]
          - 4: []
          */

         using Stream testFile = OpenTestFile("list_empty_alt.parquet");
         await using (ParquetFile reader = await ParquetFile.OpenAsync(testFile))
         {
            RowGroup rg = reader.RowGroups.First();

            Assert.Equal(4, rg.RowCount);
            DataField[] fs = reader.Schema.GetDataFields();

            DataColumn id = await rg.ReadAsync(fs[0]);
            Assert.Equal(4, id.Data.Length);
            Assert.False(id.HasRepetitions);

            DataColumn list = await rg.ReadAsync(fs[1]);
            Assert.Equal(8, list.Data.Length);
            Assert.Equal(new int[] { 0, 1, 1, 0, 0, 1, 1, 0 }, list.RepetitionLevels);
         }

      }
   }
}