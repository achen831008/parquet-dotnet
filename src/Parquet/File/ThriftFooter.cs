using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Parquet.Data;
using Parquet.File.Data;
using Parquet.Thrift;

namespace Parquet.File
{
   class ThriftFooter
   {
      private readonly Thrift.FileMetaData _fileMeta;
      private readonly ThriftSchemaTree _tree;

      class MetadataDictionary : IDictionary<string, string>
      {
         private readonly FileMetaData _fm;

         public MetadataDictionary(FileMetaData fm)
         {
            _fm = fm;
            if (_fm.Key_value_metadata == null) _fm.Key_value_metadata = new List<KeyValue>();
         }

         public string this[string key]
         {
            get
            {
               TryGetValue(key, out string value);
               return value;
            }

            set
            {
               Add(key, value);
            }
         }

         public ICollection<string> Keys => throw new NotImplementedException();

         public ICollection<string> Values => throw new NotImplementedException();

         public int Count => _fm.Key_value_metadata.Count;

         public bool IsReadOnly => false;

         public void Add(string key, string value)
         {
            if (key is null)
               throw new ArgumentNullException(nameof(key));

            Remove(key);

            _fm.Key_value_metadata.Add(new KeyValue { Key = key, Value = value });
         }

         public void Add(KeyValuePair<string, string> item)
         {
            Add(item.Key, item.Value);
         }

         public void Clear()
         {
            _fm.Key_value_metadata.Clear();
         }

         public bool Contains(KeyValuePair<string, string> item)
         {
            return _fm.Key_value_metadata.Any(i => i.Key == item.Key && i.Value == item.Value);
         }

         public bool ContainsKey(string key)
         {
            return _fm.Key_value_metadata.Any(i => i.Key == key);
         }

         public void CopyTo(KeyValuePair<string, string>[] array, int arrayIndex)
         {
            throw new NotImplementedException();
         }

         public bool Remove(string key)
         {
            for(int i = _fm.Key_value_metadata.Count - 1; i >= 0; i--)
            {
               if(_fm.Key_value_metadata[i].Key == key)
               {
                  _fm.Key_value_metadata.RemoveAt(i);
                  return true;
               }
            }

            return false;
         }

         public bool Remove(KeyValuePair<string, string> item)
         {
            throw new NotImplementedException();
         }

         public bool TryGetValue(string key, [MaybeNullWhen(false)] out string value)
         {
            KeyValue entry = _fm.Key_value_metadata.FirstOrDefault(k => k.Key == key);
            if (entry == null)
            {
               value = null;
               return false;
            }

            value = entry.Value;
            return true;
         }

         IEnumerator IEnumerable.GetEnumerator()
         {
            return CreateEnumerator();
         }

         public IEnumerator<KeyValuePair<string, string>> GetEnumerator()
         {
            return CreateEnumerator();
         }

         private IEnumerator<KeyValuePair<string, string>> CreateEnumerator()
         {
            return _fm.Key_value_metadata.Select(k => new KeyValuePair<string, string>(k.Key, k.Value)).GetEnumerator();
         }
      }

      public ThriftFooter(Thrift.FileMetaData fileMeta)
      {
         _fileMeta = fileMeta ?? throw new ArgumentNullException(nameof(fileMeta));
         _tree = new ThriftSchemaTree(_fileMeta.Schema);
         Metadata = new MetadataDictionary(_fileMeta);
      }

      public ThriftFooter(Schema schema, long totalRowCount)
      {
         if (schema == null)
         {
            throw new ArgumentNullException(nameof(schema));
         }

         _fileMeta = CreateThriftSchema(schema);
         _fileMeta.Num_rows = totalRowCount;

         _fileMeta.Created_by = $"Parquet.Net";
         _tree = new ThriftSchemaTree(_fileMeta.Schema);
         Metadata = new MetadataDictionary(_fileMeta);
      }

      public IDictionary<string, string> Metadata { get; }

      public void Add(long totalRowCount)
      {
         _fileMeta.Num_rows += totalRowCount;
      }

      public void Update()
      {
         _fileMeta.Num_rows = _fileMeta.Row_groups.Sum(rg => rg.Num_rows);
      }

      public async Task<long> Write(ThriftStream thriftStream)
      {
         int l = await thriftStream.WriteAsync(_fileMeta);
         return l;
      }

      public Thrift.SchemaElement GetSchemaElement(Thrift.ColumnChunk columnChunk)
      {
         if (columnChunk == null)
         {
            throw new ArgumentNullException(nameof(columnChunk));
         }

         List<string> path = columnChunk.Meta_data.Path_in_schema;

         int i = 0;
         foreach (string pp in path)
         {
            while (i < _fileMeta.Schema.Count)
            {
               if (_fileMeta.Schema[i].Name == pp) break;

               i++;
            }
         }

         return _fileMeta.Schema[i];
      }

      public List<string> GetPath(Thrift.SchemaElement schemaElement)
      {
         var path = new List<string>();

         ThriftSchemaTree.Node wrapped = _tree.Find(schemaElement);
         while (wrapped.parent != null)
         {
            path.Add(wrapped.element.Name);
            wrapped = wrapped.parent;
         }

         path.Reverse();
         return path;
      }

      // could use value tuple, would that nuget ref be ok to bring in?
      readonly Dictionary<StringListComparer, Tuple<int, int>> _memoizedLevels = new Dictionary<StringListComparer, Tuple<int, int>>();

      public void GetLevels(Thrift.ColumnChunk columnChunk, out int maxRepetitionLevel, out int maxDefinitionLevel)
      {
         maxRepetitionLevel = 0;
         maxDefinitionLevel = 0;

         int i = 0;
         List<string> path = columnChunk.Meta_data.Path_in_schema;

         var comparer = new StringListComparer(path);
         if (_memoizedLevels.TryGetValue(comparer, out Tuple<int, int> t))
         {
            maxRepetitionLevel = t.Item1;
            maxDefinitionLevel = t.Item2;
            return;
         }

         int fieldCount = _fileMeta.Schema.Count;

         foreach (string pp in path)
         {
            while (i < fieldCount)
            {
               SchemaElement schemaElement = _fileMeta.Schema[i];
               if (string.CompareOrdinal(schemaElement.Name, pp) == 0)
               {
                  Thrift.SchemaElement se = schemaElement;

                  bool repeated = (se.__isset.repetition_type && se.Repetition_type == Thrift.FieldRepetitionType.REPEATED);
                  bool defined = (se.Repetition_type == Thrift.FieldRepetitionType.REQUIRED);

                  if (repeated) maxRepetitionLevel += 1;
                  if (!defined) maxDefinitionLevel += 1;

                  break;
               }

               i++;
            }
         }

         _memoizedLevels.Add(comparer, Tuple.Create(maxRepetitionLevel, maxDefinitionLevel));
      }

      public Thrift.SchemaElement[] GetWriteableSchema()
      {
         return _fileMeta.Schema.Where(tse => tse.__isset.type).ToArray();
      }

      public Thrift.RowGroup AddRowGroup()
      {
         var rg = new Thrift.RowGroup { Columns = new List<ColumnChunk>() };
         if (_fileMeta.Row_groups == null) _fileMeta.Row_groups = new List<Thrift.RowGroup>();
         _fileMeta.Row_groups.Add(rg);
         return rg;
      }

      public Thrift.ColumnChunk CreateColumnChunk(CompressionCodec codec, Stream output, Thrift.Type columnType, List<string> path, int valuesCount)
      {
         var chunk = new Thrift.ColumnChunk();
         long startPos = output.Position;
         chunk.File_offset = startPos;
         chunk.Meta_data = new Thrift.ColumnMetaData();
         chunk.Meta_data.Num_values = valuesCount;
         chunk.Meta_data.Type = columnType;
         chunk.Meta_data.Codec = codec;
         chunk.Meta_data.Data_page_offset = startPos;
         chunk.Meta_data.Encodings = new List<Thrift.Encoding>
         {
            Thrift.Encoding.RLE,
            Thrift.Encoding.BIT_PACKED,
            Thrift.Encoding.PLAIN
         };
         chunk.Meta_data.Path_in_schema = path;
         chunk.Meta_data.Statistics = new Thrift.Statistics();

         return chunk;
      }

      public Thrift.PageHeader CreateDataPage(int valueCount)
      {
         var ph = new Thrift.PageHeader(Thrift.PageType.DATA_PAGE, 0, 0);
         ph.Data_page_header = new Thrift.DataPageHeader
         {
            Encoding = Thrift.Encoding.PLAIN,
            Definition_level_encoding = Thrift.Encoding.RLE,
            Repetition_level_encoding = Thrift.Encoding.RLE,
            Num_values = valueCount,
            Statistics = new Thrift.Statistics()
         };

         return ph;
      }

      #region [ Conversion to Model Schema ]

      public Schema CreateModelSchema(Options formatOptions)
      {
         int si = 0;
         Thrift.SchemaElement tse = _fileMeta.Schema[si++];
         var container = new List<Field>();

         CreateModelSchema(null, container, tse.Num_children, ref si, formatOptions);

         return new Schema(container);
      }

      private void CreateModelSchema(string path, IList<Field> container, int childCount, ref int si, Options formatOptions)
      {
         for (int i = 0; i < childCount && si < _fileMeta.Schema.Count; i++)
         {
            Thrift.SchemaElement tse = _fileMeta.Schema[si];
            IDataTypeHandler dth = DataTypeFactory.Match(tse, formatOptions);

            if (dth == null)
            {
               throw new InvalidOperationException($"cannot find data type handler to create model schema for {tse.Describe()}");
            }

            Field se = dth.CreateSchemaElement(_fileMeta.Schema, ref si, out int ownedChildCount);

            se.Path = string.Join(Schema.PathSeparator, new[] { path, se.Path ?? se.Name }.Where(p => p != null));

            if (ownedChildCount > 0)
            {
               var childContainer = new List<Field>();
               CreateModelSchema(se.Path, childContainer, ownedChildCount, ref si, formatOptions);
               foreach (Field cse in childContainer)
               {
                  se.Assign(cse);
               }
            }


            container.Add(se);
         }
      }

      private void ThrowNoHandler(Thrift.SchemaElement tse)
      {
         string ct = tse.__isset.converted_type
            ? $" ({tse.Converted_type})"
            : null;

         string t = tse.__isset.type
            ? $"'{tse.Type}'"
            : "<unspecified>";

         throw new NotSupportedException($"cannot find data type handler for schema element '{tse.Name}' (type: {t}{ct})");
      }

      #endregion

      #region [ Convertion from Model Schema ]

      public Thrift.FileMetaData CreateThriftSchema(Schema schema)
      {
         var meta = new Thrift.FileMetaData();
         meta.Version = 1;
         meta.Schema = new List<Thrift.SchemaElement>();
         meta.Row_groups = new List<Thrift.RowGroup>();

         Thrift.SchemaElement root = AddRoot(meta.Schema);
         CreateThriftSchema(schema.Fields, root, meta.Schema);

         return meta;
      }

      private Thrift.SchemaElement AddRoot(IList<Thrift.SchemaElement> container)
      {
         var root = new Thrift.SchemaElement("root");
         container.Add(root);
         return root;
      }

      private void CreateThriftSchema(IEnumerable<Field> ses, Thrift.SchemaElement parent, IList<Thrift.SchemaElement> container)
      {
         foreach (Field se in ses)
         {
            IDataTypeHandler handler = DataTypeFactory.Match(se);

            //todo: check that handler is found indeed

            handler.CreateThrift(se, parent, container);
         }
      }

      #endregion

      #region [ Helpers ]

      class ThriftSchemaTree
      {
         readonly Dictionary<SchemaElement, Node> _memoizedFindResults = new Dictionary<SchemaElement, Node>();

         public class Node
         {
            public Thrift.SchemaElement element;
            public List<Node> children;
            public Node parent;
         }

         public Node root;

         public ThriftSchemaTree(List<Thrift.SchemaElement> schema)
         {
            root = new Node { element = schema[0] };
            int i = 1;

            BuildSchema(root, schema, root.element.Num_children, ref i);
         }

         public Node Find(Thrift.SchemaElement tse)
         {
            if (_memoizedFindResults.TryGetValue(tse, out Node node))
            {
               return node;
            }
            node = Find(root, tse);
            _memoizedFindResults.Add(tse, node);
            return node;
         }

         private Node Find(Node root, Thrift.SchemaElement tse)
         {
            foreach (Node child in root.children)
            {
               if (child.element == tse) return child;

               if (child.children != null)
               {
                  Node cf = Find(child, tse);
                  if (cf != null) return cf;
               }
            }

            return null;
         }

         private void BuildSchema(Node parent, List<Thrift.SchemaElement> schema, int count, ref int i)
         {
            parent.children = new List<Node>();
            for (int ic = 0; ic < count; ic++)
            {
               Thrift.SchemaElement child = schema[i++];
               var node = new Node { element = child, parent = parent };
               parent.children.Add(node);
               if (child.Num_children > 0)
               {
                  BuildSchema(node, schema, child.Num_children, ref i);
               }
            }
         }
      }

      #endregion
   }
}
