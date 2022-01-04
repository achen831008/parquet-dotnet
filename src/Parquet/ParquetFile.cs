using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Parquet.Data;
using Parquet.Data.Rows;
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

      private readonly Options _options;
      private readonly Stream _s;
      private readonly bool _ownsStream;
      private readonly BinaryReader _br;
      private readonly BinaryWriter _bw;
      private readonly ThriftStream _ts;
      private Thrift.FileMetaData _meta;
      private ThriftFooter _footer;
      private readonly List<RowGroup> _rowGroups = new List<RowGroup>();
      private bool _isDirty;


      /// <summary>
      /// Opens parquet file stream for reading or writing. If stream is not empty, opens in append mode.
      /// </summary>
      /// <param name="s"></param>
      /// <param name="options"></param>
      /// <returns></returns>
      public static async Task<ParquetFile> OpenAsync(Stream s, Options? options = null)
      {
         var pf = new ParquetFile(s, options);

         await pf.InitAsync();

         return pf;
      }

      /// <summary>
      /// Creates a new parquet file
      /// </summary>
      /// <param name="s"></param>
      /// <param name="schema"></param>
      /// <param name="options"></param>
      /// <returns></returns>
      public static async Task<ParquetFile> CreateAsync(Stream s, Schema schema, Options? options = null)
      {
         var pf = new ParquetFile(s, options);

         pf.Schema = schema ?? throw new ArgumentNullException(nameof(schema));

         await pf.InitAsync();

         return pf;
      }

      /// <summary>
      /// Opens parquet file from disk for reading or writing.
      /// </summary>
      /// <param name="filePath"></param>
      /// <param name="mode">Mode to pass to <see cref="System.IO.File.Open(string, FileMode)"/></param>
      /// <param name="options"></param>
      /// <returns></returns>
      public static async Task<ParquetFile> OpenAsync(string filePath, FileMode mode = FileMode.OpenOrCreate, Options? options = null)
      {
         Stream s = System.IO.File.Open(filePath, mode);

         var pf = new ParquetFile(s, options, true);

         await pf.InitAsync();

         return pf;
      }

      /// <summary>
      /// File schema
      /// </summary>
      public Schema Schema { get; private set; }

      /// <summary>
      /// Row groups of this file
      /// </summary>
      public IReadOnlyCollection<RowGroup> RowGroups => _rowGroups;

      /// <summary>
      /// Total number of rows
      /// </summary>
      public long TotalRowCount => _meta.Num_rows;

      /// <summary>
      /// Gets custom key-value pairs for the file metadata
      /// </summary>
      public IDictionary<string, string> Metadata => _footer.Metadata;

      internal Options Options => _options;
      internal Stream NativeStream => _s;
      internal ThriftStream ThriftStream => _ts;
      internal ThriftFooter Footer => _footer;

      private ParquetFile(Stream s, Options? options = null, bool ownsStream = false)
      {
         _s = s ?? throw new ArgumentNullException(nameof(s));
         _options = options ?? new Options();
         _ownsStream = ownsStream;
         _br = new BinaryReader(s);
         _bw = s.CanWrite ? new BinaryWriter(s) : null;
         _ts = new ThriftStream(s);

         if(s.Length > 0)  // is this read or append?
         {
            if (!s.CanRead || !s.CanSeek) throw new ArgumentException("stream must be readable and seekable", nameof(s));
            if (s.Length <= 8) throw new IOException("not a Parquet file (size too small)");

            ValidateFile();
         }
      }

      #region [ Helpers ]

      /// <summary>
      /// Reads entire file as a table
      /// </summary>
      public static async Task<Table> ReadTableFromFile(string filePath, Options? parquetOptions = null)
      {
         await using ParquetFile pf = await ParquetFile.OpenAsync(filePath);
         return await pf.ReadAsTable();
      }

      /// <summary>
      /// Reads entire stream as a table
      /// </summary>
      public static async Task<Table> ReadTableFromStream(Stream stream, Options? parquetOptions = null)
      {
         await using ParquetFile pf = await ParquetFile.OpenAsync(stream);
         return await pf.ReadAsTable();
      }

      #endregion

      private void OpenRowGroups()
      {
         _rowGroups.Clear();

         foreach(Thrift.RowGroup group in _meta.Row_groups)
         {
            var rg = new RowGroup(group, _options, _s, _footer);
            _rowGroups.Add(rg);
         }
      }

      private async Task InitAsync()
      {
         if (_s.Length > 0)
         {
            _meta = await ReadMetadataAsync();
            _footer = new ThriftFooter(_meta);

            OpenRowGroups();

            Schema = _footer.CreateModelSchema(_options);
         }
         else
         {
            _footer = new ThriftFooter(Schema, 0);

            WriteMagic();
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

      private void WriteMagic()
      {
         _s.Write(MagicBytes, 0, MagicBytes.Length);
      }

      /// <summary>
      /// Creates a new row group that can be used to add more data to this file. The row group becomes visible after
      /// commiting it i.e. calling <see cref="IAsyncDisposable.DisposeAsync"/> on the group
      /// </summary>
      /// <returns></returns>
      public WriteableRowGroup NewRowGroup()
      {
         return new WriteableRowGroup(this);
      }

      internal void Commit(WriteableRowGroup rg)
      {
         _rowGroups.Add(rg);
         _isDirty = true;
      }

      /// <summary>
      /// 
      /// </summary>
      /// <returns></returns>
      public async ValueTask DisposeAsync()
      {
         if(_isDirty)
         {
            //finalize file
            _footer.Update();
            long size = await _footer.Write(_ts);

            //metadata size
            _bw.Write((int)size);  //4 bytes

            //end magic
            WriteMagic();          //4 bytes

            _bw.Flush();
            _s.Flush();

            _isDirty = false;
         }

         if(_ownsStream)
         {
            _s.Dispose();
         }
      }
   }
}
