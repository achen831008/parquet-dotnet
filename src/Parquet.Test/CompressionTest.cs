using System.Threading.Tasks;
using Parquet.Data;
using Parquet.Thrift;
using Xunit;

namespace Parquet.Test
{
   public class CompressionTest : TestBase
   {
      [Theory]
      [InlineData(CompressionCodec.UNCOMPRESSED)]
      [InlineData(CompressionCodec.SNAPPY)]
      [InlineData(CompressionCodec.GZIP)]
      [InlineData(CompressionCodec.LZO)]
      [InlineData(CompressionCodec.BROTLI)]
      [InlineData(CompressionCodec.LZ4)]
      [InlineData(CompressionCodec.ZSTD)]
      [InlineData(CompressionCodec.LZ4_RAW)]
      public async Task All_compression_methods_supported_for_simple_integeres(CompressionCodec codec)
      {
         const int value = 5;
         object actual = await WriteReadSingle(new DataField<int>("id"), value, codec);
         Assert.Equal(5, (int)actual);
      }

      [Theory]
      [InlineData(CompressionCodec.UNCOMPRESSED)]
      [InlineData(CompressionCodec.SNAPPY)]
      [InlineData(CompressionCodec.GZIP)]
      [InlineData(CompressionCodec.LZO)]
      [InlineData(CompressionCodec.BROTLI)]
      [InlineData(CompressionCodec.LZ4)]
      [InlineData(CompressionCodec.ZSTD)]
      [InlineData(CompressionCodec.LZ4_RAW)]
      public async Task All_compression_methods_supported_for_simple_strings(CompressionCodec codec)
      {
         /*
          * uncompressed: length - 14, levels - 6
          * 
          * 
          */

         const string value = "five";
         object actual = await WriteReadSingle(new DataField<string>("id"), value, codec);
         Assert.Equal("five", actual);
      }

      // not supported at the moment since v4. Maybe introduce an option
      /*[Theory]
      [InlineData(-1)]
      [InlineData(0)]
      [InlineData(1)]
      [InlineData(2)]
      public async Task Gzip_all_levels(int level)
      {
         const string value = "five";
         object actual = await WriteReadSingle(new DataField<string>("id"), value, CompressionMethod.Gzip, level);
         Assert.Equal("five", actual);
      }*/
   }
}
