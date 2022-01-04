using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Parquet.File
{
   class DataPageCodec
   {
      public static async byte[] ReadAsync(
         Stream source,
         Thrift.CompressionCodec codec,
         int compressedLength,
         int uncompressedLength,
         ArrayPool<byte> arrayPool)
      {
         // luckily, I know exactly how much to allocate - compressedLength

         byte[] data = arrayPool.Rent(compressedLength);
         int read = await source.ReadAsync(data, 0, compressedLength);

         if (read != compressedLength)
            throw new IOException($"expected to read {compressedLength}b not {read}b");

         if(codec == Thrift.CompressionCodec.UNCOMPRESSED)
            return (data, uncompressedLength);

         try
         {
            return Decode(
               codec, data.AsSpan(0, compressedLength), uncompressedLength, arrayPool);
         }
         finally
         {
            // return encoded data back
            arrayPool.Return(data);
         }
      }

      private static byte[] Decode(Thrift.CompressionCodec codec,
         Span<byte> encoded,
         int decodedLength,
         ArrayPool<byte> arrayPool)
      {
         // gzip and brotli are part of .net runtime

         if (codec == Thrift.CompressionCodec.GZIP)
         {
            encoded.ToArray().Gzip()
         }
         else if (codec == Thrift.CompressionCodec.BROTLI)
         {

         }
         else
         {
            // use bundled native codecs
            return Codecs.Decode(codec, encoded, arrayPool, out int outputSizeRet);
         }
      }
   }
}
