using NpgsqlTypes;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MergeServer.Core
{
    /// <summary>
    /// Mark database batch operating attribute.
    /// </summary>
    public class DbBulkAttribute : Attribute
    {
        /// <summary>
        /// Whether to ignore fileds.
        /// </summary>
        public bool Ignore { get; set; }

        /// <summary>
        /// columns name.default to lower case if no set other columns name
        /// </summary>
        public string ColumnName { get; set; }
        /// <summary>
        /// data type,default text.
        /// for Npgsql type.
        /// </summary>
        public NpgsqlDbType Type { get; set; } = NpgsqlDbType.Text;
    }
}
