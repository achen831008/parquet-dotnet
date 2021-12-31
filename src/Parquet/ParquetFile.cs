using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Parquet.File;

namespace Parquet
{
   /// <summary>
   /// Entry point to this library
   /// </summary>
   public class ParquetFile : IAsyncDisposable
   {
      private const string MagicString = "PAR1";
      private static readonly byte[] MagicBytes = Encoding.ASCII.GetBytes(MagicString);

      private readonly Stream _s;
      private readonly BinaryReader _br;
      private readonly BinaryWriter _bw;
      private readonly ThriftStream _ts;
      private Thrift.FileMetaData _meta;
      private ThriftFooter _footer;


      /// <summary>
      /// Opens parquet file stream for reading or writing. If stream is not empty, opens in append mode.
      /// </summary>
      /// <param name="s"></param>
      /// <returns></returns>
      public static async Task<ParquetFile> OpenAsync(Stream s)
      {
         var pf = new ParquetFile(s);

         await pf.InitAsync();

         return pf;
      }

      /// <summary>
      /// Gets the number of rows groups in this file
      /// </summary>
      public int RowGroupCount => _meta.Row_groups.Count;

      private ParquetFile(Stream s)
      {
         _s = s;
         _br = new BinaryReader(s);
         _bw = new BinaryWriter(s);
         _ts = new ThriftStream(s);

         if(s.Length > 0)  // is this read or append?
         {
            if (!s.CanRead || !s.CanSeek) throw new ArgumentException("stream must be readable and seekable", nameof(s));
            if (s.Length <= 8) throw new IOException("not a Parquet file (size too small)");

            ValidateFile();
         }
      }

      /// <summary>
      /// Opens a specific row groups
      /// </summary>
      /// <param name="index"></param>
      /// <returns></returns>
      /// <exception cref="ArgumentException"></exception>
      public RowGroup OpenRowGroup(uint index)
      {
         if (index >= _meta.Row_groups.Count)
            throw new ArgumentException(
               $"there are only {_meta.Row_groups.Count} row groups but index {index} was requested", nameof(index));

         return new RowGroup(_meta.Row_groups[(int)index], _footer, _s, _ts, null);
      }

      private async Task InitAsync()
      {
         if (_s.Length > 0)
         {
            _meta = await ReadMetadataAsync();
            _footer = new ThriftFooter(_meta);
         }
      }

      void ValidateFile()
      {
         _s.Seek(0, SeekOrigin.Begin);
         char[] head = _br.ReadChars(4);
         string shead = new string(head);
         _s.Seek(-4, SeekOrigin.End);
         char[] tail = _br.ReadChars(4);
         string stail = new string(tail);
         if (shead != MagicString)
            throw new IOException($"not a Parquet file(head is '{shead}')");
         if (stail != MagicString)
            throw new IOException($"not a Parquet file(head is '{stail}')");
      }

      void GoToBeginning() => _s.Seek(0, SeekOrigin.Begin);

      void GoToEnd() => _s.Seek(0, SeekOrigin.End);

      void GoBeforeFooter()
      {
         //go to -4 bytes (PAR1) -4 bytes (footer length number)
         _s.Seek(-8, SeekOrigin.End);
         int footerLength = _br.ReadInt32();

         //set just before footer starts
         _s.Seek(-8 - footerLength, SeekOrigin.End);
      }

      Task<Thrift.FileMetaData> ReadMetadataAsync()
      {
         GoBeforeFooter();

         return _ts.ReadAsync<Thrift.FileMetaData>();
      }

      /// <summary>
      /// 
      /// </summary>
      /// <returns></returns>
      public ValueTask DisposeAsync()
      {
         return ValueTask.CompletedTask;
      }
   }
}
