# Breaking changes in v4

The majority of v4 interface was rewritten from ground up in order to:

- Simplify library usage by adopting to the most common use cases.
- Drop functionality I can't afford to continue working on (this is my free personal project after all!).
- Improve performance and utilise the latest advancements in .NET runtime.
- Make it fully asynchronous in order to be able to scale out during heavy workloads.

Good luck!

## General improvements

- In order to become more cross-platform, the default compression method when writing files is **Uncompressed**. This makes sure the library will work on any hardware.
- v4 supports all [compression methods](compression.md) as per latest parquet specification with [minor caveats](compression.md).
- All the public methods are now asynchronous.
