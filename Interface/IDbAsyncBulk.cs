using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MergeServer.Core
{
    public interface IDbAsyncBulk
    {
        /// <summary>
        /// default init.
        /// use reflect to auto init all type, to lower case database fileds,and  default basic type.
        /// if ignore some fileds,please use DbBulk,Ignore property to remarkable fileds.
        /// if other operating,need user-defined to init operate.
        /// </summary>
        /// <typeparam name="T">Corresponding type</typeparam>
        Task InitDefaultMappings<T>();

        /// <summary>
        /// batch operating
        /// </summary>
        /// <typeparam name="T">will operate object entity type.</typeparam>
        /// <param name="connection_string">database connecting string.</param>
        /// <param name="targetTable">target table name. </param>
        /// <param name="list">will operate data list.</param>
        Task CopyToServer<T>(string connection_string, string targetTable, List<T> list);

        /// <summary>
        /// batch operating
        /// </summary>
        /// <typeparam name="T">will operate object entity type.</typeparam>
        /// <param name="connection">database connecting string.need to check database connecting is openning.
        /// if nothing other follow-up operate, shouldn't cover this connecting.</param>
        /// <param name="targetTable">target table name.</param>
        /// <param name="list">will operate data list.</param>
        Task CopyToServer<T>(DbConnection connection, string targetTable, List<T> list);

        /// <summary>
        /// renew as it exists，insert as it not exists.
        /// follow up : 
        /// 1.create temporary table
        /// 2.put data into temporary table.
        /// 3.merge data to target table.
        /// </summary>
        /// <typeparam name="T">data type</typeparam>
        /// <param name="connection_string">connecting string</param>
        /// <param name="keys">mapping orignal table and target table fileds,need primary key and data only,if not will throw error.</param>
        /// <param name="targetTable">target table name.</param>
        /// <param name="list">will operate data list.</param>
        /// <param name="tempTable">put data into temporary table,default name as 'target table name + # or _temp'</param>
        /// <param name="insertmapping">need to insert column，if is null,just use Mapping fileds，in order to avoid auto-create column</param>
        /// <param name="updatemapping">need to modify column，if is null,just use Mapping fileds</param>
        Task MergeToServer<T>(string connection_string, List<string> keys, string targetTable, List<T> list, string tempTable = null, List<string> insertmapping = null, List<string> updatemapping = null);

        /// <summary>
        /// renew as it exists，insert as it not exists.
        /// follow up : 
        /// 1.create temporary table
        /// 2.put data into temporary table.
        /// 3.merge data to target table.
        /// </summary>
        /// <typeparam name="T">data type</typeparam>
        /// <param name="connection">database connecting string.need to check database connecting is openning.</param>
        /// <param name="keys">mapping orignal table and target table fileds,need primary key and data only,if not will throw error.</param>
        /// <param name="targetTable">target table name.</param>
        /// <param name="list">will operate data list.</param>
        /// <param name="tempTable">put data into temporary table,default name as 'target table name + # or _temp'</param>
        /// <param name="insertmapping">need to insert column，if is null,just use Mapping fileds，in order to avoid auto-create column</param>
        /// <param name="updatemapping">need to modify column，if is null,just use Mapping fileds</param>
        Task MergeToServer<T>(DbConnection connection, List<string> keys, string targetTable, List<T> list, string tempTable = null, List<string> insertmapping = null, List<string> updatemapping = null);

        /// <summary>
        ///  batch update operating。
        /// 1.create temporary table
        /// 2.put data into temporary table.
        /// 3.merge data to target table.
        /// </summary>
        /// <typeparam name="T">data type</typeparam>
        /// <param name="connection_string">connecting string</param>
        /// <param name="where_name">matching 'where' compare fileds.</param>
        /// <param name="update_name">need to update fileds.</param>
        /// <param name="targetTable">target table name</param>
        /// <param name="list">will operate data list.</param>
        /// <param name="tempTable">put data into temporary table,default name as 'target table name + # or _temp'</param>
        Task UpdateToServer<T>(string connection_string, List<string> where_name, List<string> update_name, string targetTable, List<T> list, string tempTable = null);

        /// <summary>
        ///  batch update operating。
        /// 1.create temporary table
        /// 2.put data into temporary table.
        /// 3.merge data to target table.
        /// </summary>
        /// <typeparam name="T">data type</typeparam>
        /// <param name="connection_string">connecting string</param>
        /// <param name="where_name">matching 'where' compare fileds.</param>
        /// <param name="update_name">need to update fileds.</param>
        /// <param name="targetTable">target table name</param>
        /// <param name="list">will operate data list.</param>
        /// <param name="tempTable">put data into temporary table,default name as 'target table name + # or _temp'</param>
        /// <param name="createtemp"> create temporary table or not </param>
        Task UpdateToServer<T>(DbConnection connection, List<string> where_name, List<string> update_name, string targetTable, List<T> list, string tempTable = null, bool createtemp = true);

        /// <summary>
        /// renew as it exists，insert as it not exists.original table not exist and  target table exist will remove.
        /// 1.create temporary table
        /// 2.put data into temporary table.
        /// 3.merge data to target table.
        /// 4.will remove data that temporary data not exist and target table exist.
        /// </summary>
        /// <typeparam name="T">data type</typeparam>
        /// <param name="connection_string">connecting string</param>
        /// <param name="keys">mapping orignal table and target table fileds,need primary key and data only,if not will throw error.</param>
        /// <param name="targetTable">target table name</param>
        /// <param name="list">will operate data list.</param>
        /// <param name="tempTable">put data into temporary table,default name as 'target table name + # or _temp'</param>
        /// <param name="insertmapping">need to insert column，if is null,just use Mapping fileds，in order to avoid auto-create column</param>
        /// <param name="updatemapping">need to modify column，if is null,just use Mapping fileds</param>
        Task MergeAndDeleteToServer<T>(string connection_string, List<string> keys, string targetTable, List<T> list, string tempTable = null, List<string> insertmapping = null, List<string> updatemapping = null);

        /// <summary>
        /// renew as it exists，insert as it not exists.original table not exist and  target table exist will remove.
        ///  1.create temporary table
        /// 2.put data into temporary table.
        /// 3.merge data to target table.
        /// 4.will remove data that temporary data not exist and target table exist.
        /// </summary>
        /// <typeparam name="T">data type</typeparam>
        /// <param name="connection">database connecting string.need to check database connecting is openning.</param>
        /// <param name="keys">mapping orignal table and target table fileds,need primary key and data only,if not will throw error.</param>
        /// <param name="targetTable">target table name</param>
        /// <param name="list">will operate data list.</param>
        /// <param name="tempTable">put data into temporary table,default name as 'target table name + # or _temp'</param>
        /// <param name="insertmapping">need to insert column，if is null,just use Mapping fileds，in order to avoid auto-create column</param>
        /// <param name="updatemapping">need to modify column，if is null,just use Mapping fileds</param>
        Task MergeAndDeleteToServer<T>(DbConnection connection, List<string> keys, string targetTable, List<T> list, string tempTable = null, List<string> insertmapping = null, List<string> updatemapping = null);

        /// <summary>
        /// create temporary table
        /// </summary>
        /// <param name="tempTable">create temporary table name</param>
        /// <param name="targetTable">rarget table name</param>
        /// <param name="connection">database connecting</param>
        Task CreateTempTable(string tempTable, string targetTable, DbConnection connection);
    }
}
