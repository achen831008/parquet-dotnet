using System;
using System.Buffers;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;
using Parquet.Thrift;

namespace Parquet
{
   /// <summary>
   /// Cross-platform P/Invoke wrapper as described in https://docs.microsoft.com/en-us/dotnet/standard/native-interop/cross-platform
   /// </summary>
   static class Codecs
   {
      const string LibName = "codecs";

      [DllImport(LibName)]
      static unsafe extern bool encode(int codec,
         byte* inputBuffer,
         int inputBufferSize,
         byte* outputBuffer,
         int* outputBufferSize);

      [DllImport(LibName)]
      static unsafe extern bool decode(int codec,
         byte* inputBuffer,
         int inputBufferSize,
         byte* outputBuffer,
         int* outputBufferSize);

      public static byte[] Encode(
         CompressionCodec codec,
         ReadOnlySpan<byte> input,
         ArrayPool<byte> allocPool,
         out int outputSizeRet)
      {
         int outputSize = 0;

         unsafe
         {
            fixed (byte* inputPtr = input)
            {
               bool ok = encode(
                  (int)codec, inputPtr, input.Length, null, &outputSize);
               if (!ok)
               {
                  outputSizeRet = 0;
                  return null;
               }

               byte[] output = allocPool.Rent(outputSize);
               //outputSize = output.Length;

               fixed (byte* outputPtr = output)
               {
                  ok = encode(
                     (int)codec, inputPtr, input.Length, outputPtr, &outputSize);

                  outputSizeRet = outputSize;
                  return output;
               }
            }
         }
      }

      public static byte[] Decode(
         CompressionCodec codec,
         ReadOnlySpan<byte> input,
         ArrayPool<byte> allocPool,
         out int outputSizeRet)
      {
         int outputSize = 0;

         unsafe
         {
            fixed (byte* inputPtr = input)
            {
               bool ok = decode(
                  (int)codec, inputPtr, input.Length, null, &outputSize);
               if (!ok)
               {
                  outputSizeRet = 0;
                  return null;
               }

               byte[] output = allocPool.Rent(outputSize);
               //outputSize = output.Length;

               fixed (byte* outputPtr = output)
               {
                  ok = decode(
                     (int)codec, inputPtr, input.Length, outputPtr, &outputSize);

                  outputSizeRet = outputSize;
                  return output;
               }
            }
         }
      }
   }
}
