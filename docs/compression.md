# Compression

v4 supports **all** compression methods as per parquet specification. `GZIP` and `BROTLI` are supported on all platforms where .NET runs, whereas the rest of the compression methods are packaged into `native-codecs` library and are available on all major OSes and is using recommended compression libraries as per [compression specs](https://github.com/apache/parquet-format/blob/master/Compression.md). 

| Index | Method       | built-in           | native-codecs      |
| ----- | ------------ | ------------------ | ------------------ |
| 0     | UNCOMPRESSED | :white_check_mark: |                    |
| 1     | SNAPPY       |                    | :white_check_mark: |
| 2     | GZIP         | :white_check_mark: |                    |
| 3     | LZO          |                    | :white_check_mark: |
| 4     | BROTLI       | :white_check_mark: |                    |
| 5     | LZ4          |                    | :white_check_mark: |
| 6     | ZSTD         |                    | :white_check_mark: |
| 7     | LZ4_RAW      |                    | :white_check_mark: |

This does mean that anything other than `gzip` and `brotli` won't work if you're not using Linux x64, MacOS x64 or Windows x64, unless you compile native codecs for that architecture. Majority of parquet users are running their heaviest workloads on x64 Linux.
