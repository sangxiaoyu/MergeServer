using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MergeServer.Core
{
    /// <summary>
    /// 数据库批量操作标记，用于标记对象属性。
    /// </summary>
    public class DbBulkAttribute : Attribute
    {
        /// <summary>
        /// 是否忽略。忽略则其余属性不需要设置，不忽略则必须设置Type。
        /// </summary>
        public bool Ignore { get; set; }

        /// <summary>
        /// 列名，不设置则默认为实体字段名小写
        /// </summary>
        public string ColumnName { get; set; }

    }
}
