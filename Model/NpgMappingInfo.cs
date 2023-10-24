using NpgsqlTypes;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MergeServer.Core
{
    /// <summary>
    /// 映射信息
    /// </summary>
    public class NpgMappingInfo
    {
        /// <summary>
        /// 映射的字段名
        /// </summary>
        public string DbKey { get; set; }

        /// <summary>
        /// 对象名
        /// </summary>
        public string ObjectKey { get; set; }

        /// <summary>
        /// 映射的字段类型，默认为NpgsqlTypes.NpgsqlDbType.Text
        /// </summary>
        public NpgsqlDbType Type { get; set; } = NpgsqlDbType.Text;

    }
}
