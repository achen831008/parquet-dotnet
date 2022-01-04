using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using NetBox.IO;
using Parquet.Data;
using Xunit;

namespace Parquet.Test
{
   public class NonSeekableWriterTest
   {
      [Fact]
      public async Task Write_multiple_row_groups_to_forward_only_stream()
      {
         var ms = new MemoryStream();
         var forwardOnly = new WriteableNonSeekableStream(ms);

         var schema = new Schema(
            new DataField<int>("id"),
            new DataField<string>("nonsense"));

         await using (ParquetFile writer = await ParquetFile.CreateAsync(forwardOnly, schema))
         {
            using (WriteableRowGroup rgw = writer.NewRowGroup())
            {
               await rgw.WriteAsync(new DataColumn((DataField)schema[0], new[] { 1 }));
               await rgw.WriteAsync(new DataColumn((DataField)schema[1], new[] { "1" }));
            }

            using (WriteableRowGroup rgw = writer.NewRowGroup())
            {
               await rgw.WriteAsync(new DataColumn((DataField)schema[0], new[] { 2 }));
               await rgw.WriteAsync(new DataColumn((DataField)schema[1], new[] { "2" }));
            }
         }

         ms.Position = 0;
         await using (ParquetFile reader = await ParquetFile.OpenAsync(ms))
         {
            Assert.Equal(2, reader.RowGroups.Count);

            RowGroup rgr = reader.RowGroups.First();
            Assert.Equal(1, rgr.RowCount);

            DataColumn column = await rgr.ReadAsync((DataField)schema[0]);
            Assert.Equal(1, column.Data.GetValue(0));

            rgr = reader.RowGroups.ElementAt(1);
            Assert.Equal(1, rgr.RowCount);

            column = await rgr.ReadAsync((DataField)schema[0]);
            Assert.Equal(2, column.Data.GetValue(0));
         }
      }

      class WriteableNonSeekableStream : DelegatedStream
      {
         public WriteableNonSeekableStream(Stream master) : base(master)
         {
         }

         public override bool CanSeek => false;

         public override bool CanRead => true;

         public override long Seek(long offset, SeekOrigin origin)
         {
            throw new NotSupportedException();
         }

         public override long Position
         {
            get => throw new NotSupportedException();
            set => throw new NotSupportedException();
         }
      }

   }
}