using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using IronSnappy;
using Parquet.File.Streams;

namespace Parquet.File
{
   static class DataStreamFactory
   {
      private static ArrayPool<byte> BytesPool = ArrayPool<byte>.Shared;

      // this will eventually disappear once we fully migrate to System.Memory
      public static GapStream CreateWriter(
         Stream nakedStream,
         Thrift.CompressionCodec compressionMethod, int compressionLevel,
         bool leaveNakedOpen)
      {
         Stream dest;

         /*#if !NET14
                  nakedStream = new BufferedStream(nakedStream); //optimise writer performance
         #endif*/

         switch (compressionMethod)
         {
            case Thrift.CompressionCodec.GZIP:
               dest = new GZipStream(nakedStream, ToGzipCompressionLevel(compressionLevel), leaveNakedOpen);
               leaveNakedOpen = false;
               break;
            case Thrift.CompressionCodec.SNAPPY:
               dest = new SnappyInMemoryStream(nakedStream, CompressionMode.Compress);
               leaveNakedOpen = false;
               break;
            case Thrift.CompressionCodec.UNCOMPRESSED:
               dest = nakedStream;
               break;
            default:
               throw new NotImplementedException($"unknown compression method {compressionMethod}");
         }

         return new GapStream(dest, leaveOpen: leaveNakedOpen);
      }

      private static CompressionLevel ToGzipCompressionLevel(int compressionLevel)
      {
         switch (compressionLevel)
         {
            case 0:
               return CompressionLevel.NoCompression;
            case 1:
               return CompressionLevel.Fastest;
            case 2:
               return CompressionLevel.Optimal;
            default:
               return CompressionLevel.Optimal;
         }
      }

      public static BytesOwner ReadPageData(Stream nakedStream, Thrift.CompressionCodec compressionMethod,
         int compressedLength, int uncompressedLength)
      {
         int totalBytesRead = 0;
         int currentBytesRead = int.MinValue;
         byte[] data = BytesPool.Rent(compressedLength);
         bool dataRented = true;

         // Some storage solutions (like Azure blobs) might require more than one 'Read' action to read the requested length.
         while (totalBytesRead < compressedLength && currentBytesRead != 0)
         {
            currentBytesRead = nakedStream.Read(data, totalBytesRead, compressedLength - totalBytesRead);
            totalBytesRead += currentBytesRead;
         }

         if (totalBytesRead != compressedLength)
         {
            throw new ParquetException($"expected {compressedLength} bytes in source stream but could read only {totalBytesRead}");
         }

         switch (compressionMethod)
         {
            case Thrift.CompressionCodec.UNCOMPRESSED:
               //nothing to do, original data is the raw data
               break;
            case Thrift.CompressionCodec.GZIP:
               using (var source = new MemoryStream(data, 0, compressedLength))
               {
                  byte[] unGzData = BytesPool.Rent(uncompressedLength);
                  using (var dest = new MemoryStream(unGzData, 0, uncompressedLength))
                  {
                     using (var gz = new GZipStream(source, CompressionMode.Decompress))
                     {
                        gz.CopyTo(dest);
                     }
                  }
                  BytesPool.Return(data);
                  data = unGzData;
               }
               break;
            case Thrift.CompressionCodec.SNAPPY:
               byte[] uncompressed = Snappy.Decode(data.AsSpan(0, compressedLength));
               BytesPool.Return(data);
               data = uncompressed;
               dataRented = false;
               break;
            default:
               throw new NotSupportedException("method: " + compressionMethod);
         }

         return new BytesOwner(data, 0, data.AsMemory(0, uncompressedLength), d => BytesPool.Return(d), dataRented);
      }
   }
}
