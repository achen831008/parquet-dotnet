using System;
using System.Buffers;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using NetBox.Generator;
using Parquet.Thrift;
using Xunit;

namespace Parquet.Test
{
   public class CodecsTest
   {
      [Theory]
      [InlineData(CompressionCodec.SNAPPY, new byte[] { 1, 2, 3, 4, 5, 6, 7, 8 })]
      [InlineData(CompressionCodec.SNAPPY, new byte[]
      {
         1, 2, 3, 4, 5, 6, 7, 8,
         1, 2, 3, 4, 5, 6, 7, 8,
         1, 2, 3, 4, 5, 6, 7, 8,
         1, 2, 3, 4, 5, 6, 7, 8,
         1, 2, 3, 4, 5, 6, 7, 8,
         1, 2, 3, 4, 5, 6, 7, 8,
         1, 2, 3, 4, 5, 6, 7, 8})]
      [InlineData(CompressionCodec.SNAPPY, null)]
      public void EncodeDecodeTest(CompressionCodec codec, byte[] input)
      {
         if (input == null) input = RandomGenerator.GetRandomBytes(1000, 10000);

         byte[] encoded = null;
         byte[] decoded = null;

         try
         {
            encoded = Codecs.Encode(codec,
               input,
               ArrayPool<byte>.Shared,
               out int encodeSize);

            decoded = Codecs.Decode(codec,
               encoded.AsSpan(0, encodeSize),
               ArrayPool<byte>.Shared,
               out int decodeSize);

            Assert.Equal(input.Length, decodeSize);
               
         }
         finally
         {
            if(encoded != null)
               ArrayPool<byte>.Shared.Return(encoded);

            if (decoded != null)
               ArrayPool<byte>.Shared.Return(decoded);
         }
      }
   }
}
