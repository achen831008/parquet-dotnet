using System;
using System.Collections.Generic;
using System.Linq;
using Parquet.Thrift;

namespace Parquet.File.Data
{
   static class DataFactory
   {
      private static readonly Dictionary<CompressionCodec, KeyValuePair<IDataWriter, IDataReader>> CompressionMethodToWorker = new Dictionary<CompressionCodec, KeyValuePair<IDataWriter, IDataReader>>()
      {
         { CompressionCodec.UNCOMPRESSED, new KeyValuePair<IDataWriter, IDataReader>(new UncompressedDataWriter(), new UncompressedDataReader()) },
         { CompressionCodec.GZIP, new KeyValuePair<IDataWriter, IDataReader>(new GzipDataWriter(), new GzipDataReader()) },
         { CompressionCodec.SNAPPY, new KeyValuePair<IDataWriter, IDataReader>(new SnappyDataWriter(), new SnappyDataReader()) }
      };

      public static IDataWriter GetWriter(Thrift.CompressionCodec method)
      {
         return CompressionMethodToWorker[method].Key;
      }

      public static IDataReader GetReader(Thrift.CompressionCodec method)
      {
         return CompressionMethodToWorker[method].Value;
      }
   }
}
