using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Parquet.Data;

namespace Parquet
{
   /// <summary>
   /// Represents row group reading and writing functionality.
   /// </summary>
   public class RowGroup
   {
      internal RowGroup(Thrift.RowGroup rowGroup)
      {

      }

      public async Task<DataColumn> ReadAsync(DataField field)
      {

      }
   }
}
