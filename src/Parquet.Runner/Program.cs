using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using NetBox.Performance;
using Parquet.Data;
using F = System.IO.File;

namespace Parquet.Runner
{
   class Program
   {
      static void Main(string[] args)
      {
         //CompressOld("c:\\tmp\\DSL.svg", "c:\\tmp\\DSL.svg.oldsnappy");
         //CompressNew("c:\\tmp\\DSL.svg", "c:\\tmp\\DSL.svg.newsnappy");

         //CompressOld("c:\\tmp\\rfc8660long.txt", "c:\\tmp\\rfc8660long.txt.oldsnappy");
         //CompressNew("c:\\tmp\\rfc8660long.txt", "c:\\tmp\\rfc8660long.txt.newsnappy");
         ReadPerf();
      }

      private static async Task ReadPerf()
      {
         using (ParquetReader reader = await ParquetReader.OpenFromFile(@"C:\dev\parquet-dotnet\src\Parquet.Test\data\customer.impala.parquet", new ParquetOptions { TreatByteArrayAsString = true }))
         {
            var cl = new List<DataColumn>();

            using (ParquetRowGroupReader rgr = reader.OpenRowGroupReader(0))
            {
               foreach (DataField field in reader.Schema.GetDataFields())
               {
                  DataColumn dataColumn = await rgr.ReadColumn(field);
                  cl.Add(dataColumn);
               }
            }
         }
      }

      static void CompressNew(string src, string dest)
      {
         using (FileStream streamDest = F.OpenWrite(dest))
         {
            using (Stream streamSnappy = IronSnappy.Snappy.OpenWriter(streamDest))
            {
               using(FileStream streamSrc = F.OpenRead(src))
               {
                  using(var time = new TimeMeasure())
                  {
                     streamSrc.CopyTo(streamSnappy);

                     TimeSpan duration = time.Elapsed;

                     Console.WriteLine($"new: {src} => {dest}. {duration} {new FileInfo(dest).Length}");
                  }
               }
            }
         }
      }
   }
}