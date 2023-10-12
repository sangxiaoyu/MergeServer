using FastMember;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Data.SqlClient;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace MergeServer.Core
{
    /// <summary>
    /// sql server batch
    /// </summary>
    public class SqlServerAsyncBulk : IDbAsyncBulk
    {
        /// <summary>
        /// log recoding
        /// </summary>
        private ILogger _log;

        /// <summary>
        ///batch insert size（handle a batch every time ）。default 10000。
        /// </summary>
        public int BatchSize { get; set; }

        /// <summary>
        /// overtime,default 300
        /// </summary>
        public int BulkCopyTimeout { get; set; }

        /// <summary>
        /// columns mapping
        /// </summary>
        public Dictionary<string, string> ColumnMappings { get; set; }

        /// <summary>
        /// structure function
        /// </summary>
        /// <param name="log"></param>
        public SqlServerAsyncBulk(ILogger<SqlServerAsyncBulk> log)
        {
            _log = log;
            BatchSize = 10000;
            BulkCopyTimeout = 300;
        }

        /// <summary>
        /// default init.
        /// use reflect to auto init all type, to lower case database fileds,and  default basic type.
        /// if ignore some fileds,please use DbBulk,Ignore property to remarkable fileds.
        /// if other operating,need user-defined to init operate.
        /// </summary>
        /// <typeparam name="T">Corresponding type</typeparam>
        public Task InitDefaultMappings<T>()
        {
            PropertyInfo[] properties = typeof(T).GetProperties();
            ColumnMappings = new Dictionary<string, string>();
            PropertyInfo[] array = properties;
            foreach (PropertyInfo propertyInfo in array)
            {
                object[] customAttributes = propertyInfo.GetCustomAttributes(typeof(DbBulkAttribute), inherit: false);
                if (customAttributes.Length == 0)
                {
                    ColumnMappings.Add(propertyInfo.Name, propertyInfo.Name.ToLower());
                    continue;
                }
                DbBulkAttribute dbBulkAttribute = customAttributes.First() as DbBulkAttribute;
                if (!dbBulkAttribute.Ignore)
                {
                    string value = (string.IsNullOrEmpty(dbBulkAttribute.ColumnName) ? propertyInfo.Name.ToLower() : dbBulkAttribute.ColumnName);
                    ColumnMappings.Add(propertyInfo.Name, value);
                }
            }
            return Task.CompletedTask;
        }

        /// <summary>
        /// batch operating
        /// </summary>
        /// <typeparam name="T">will operate object entity type.</typeparam>
        /// <param name="connection_string">database connecting string.</param>
        /// <param name="targetTable">target table name. </param>
        /// <param name="list">will operate data list.</param>
        public async Task CopyToServer<T>(string connection_string, string targetTable, List<T> list)
        {
            using SqlConnection connection = new SqlConnection(connection_string);
            await connection.OpenAsync();
            await CopyToServer(connection, targetTable, list);
        }

        /// <summary>
        /// batch operating
        /// </summary>
        /// <typeparam name="T">will operate object entity type.</typeparam>
        /// <param name="connection">database connecting string.need to check database connecting is openning.
        /// if nothing other follow-up operate, shouldn't cover this connecting.</param>
        /// <param name="targetTable">target table name.</param>
        /// <param name="list">will operate data list.</param>
        public async Task CopyToServer<T>(DbConnection connection, string targetTable, List<T> list)
        {
            SqlConnection connection2 = connection as SqlConnection;
            using SqlBulkCopy bulkCopy = new SqlBulkCopy(connection2, SqlBulkCopyOptions.KeepIdentity, null);
            bulkCopy.BulkCopyTimeout = BulkCopyTimeout;
            bulkCopy.DestinationTableName = targetTable;
            bulkCopy.BatchSize = BatchSize;
            if (ColumnMappings != null && ColumnMappings.Count > 0)
            {
                foreach (KeyValuePair<string, string> columnMapping in ColumnMappings)
                {
                    bulkCopy.ColumnMappings.Add(columnMapping.Key, columnMapping.Value);
                }
            }
            using ObjectReader reader = ObjectReader.Create(list, ColumnMappings.Keys.ToArray());
            await bulkCopy.WriteToServerAsync(reader);
        }

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
        public async Task MergeToServer<T>(string connection_string, List<string> keys, string targetTable, List<T> list, string tempTable = null, List<string> insertmapping = null, List<string> updatemapping = null)
        {
            using SqlConnection connection = new SqlConnection(connection_string);
            await connection.OpenAsync();
            await MergeToServer(connection, keys, targetTable, list, tempTable, insertmapping, updatemapping);
        }

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
        public async Task MergeToServer<T>(DbConnection connection, List<string> keys, string targetTable, List<T> list, string tempTable = null, List<string> insertmapping = null, List<string> updatemapping = null)
        {
            if (string.IsNullOrEmpty(tempTable))
            {
                tempTable = "#" + targetTable;
            }
            if (!tempTable.StartsWith("#"))
            {
                tempTable = "#" + tempTable;
            }
            await Merge(connection, keys, targetTable, list, tempTable, insertmapping, updatemapping, CreateMergeSql);
        }
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
        public async Task MergeAndDeleteToServer<T>(string connection_string, List<string> keys, string targetTable, List<T> list, string tempTable = null, List<string> insertmapping = null, List<string> updatemapping = null)
        {
            using SqlConnection connection = new SqlConnection(connection_string);
            await connection.OpenAsync();
            await MergeAndDeleteToServer(connection, keys, targetTable, list, tempTable, insertmapping, updatemapping);
        }

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
        /// 
        public async Task MergeAndDeleteToServer<T>(DbConnection connection, List<string> keys, string targetTable, List<T> list, string tempTable = null, List<string> insertmapping = null, List<string> updatemapping = null)
        {
            if (string.IsNullOrEmpty(tempTable))
            {
                tempTable = "#" + targetTable;
            }
            if (!tempTable.StartsWith("#"))
            {
                tempTable = "#" + tempTable;
            }
            await Merge(connection, keys, targetTable, list, tempTable, insertmapping, updatemapping, CreateMergeAndDeleteSql);
        }

        private string CreateMergeSql(string targetTable, string tempTable, string update, string keybuilder, string builder, string valuebuilder)
        {
            return "MERGE INTO " + targetTable + " AS des\r\n                                  USING " + tempTable + " AS src \r\n                                  ON(" + keybuilder + ")\r\n                                    WHEN MATCHED THEN\r\n                                     UPDATE SET  " + update + " \r\n                                    WHEN NOT MATCHED THEN\r\n                                     INSERT (" + builder + ") VALUES(" + valuebuilder + ");";
        }

        private string CreateMergeAndDeleteSql(string targetTable, string tempTable, string update, string keybuilder, string builder, string valuebuilder)
        {
            return "MERGE INTO " + targetTable + " AS des\r\n                                  USING " + tempTable + " AS src \r\n                                  ON(" + keybuilder + ")\r\n                                    WHEN MATCHED THEN\r\n                                     UPDATE SET  " + update + " \r\n                                    WHEN NOT MATCHED by target THEN\r\n                                     INSERT (" + builder + ") VALUES(" + valuebuilder + ")\r\n                                    WHEN NOT MATCHED by source THEN\r\n                                     DELETE;";
        }

        private async Task Merge<T>(DbConnection connection, List<string> keys, string targetTable, List<T> list, string tempTable, List<string> insertmapping, List<string> updatemapping, Func<string, string, string, string, string, string, string> sqlfunc)
        {
            await CreateTempTable(tempTable, targetTable, connection);
            await CopyToServer(connection, tempTable, list);
            DbCommand dbCommand = connection.CreateCommand();
            StringBuilder stringBuilder = new StringBuilder(300);
            if (insertmapping != null)
            {
                foreach (string item in insertmapping)
                {
                    stringBuilder.Append(item);
                    stringBuilder.Append(',');
                }
            }
            else
            {
                foreach (KeyValuePair<string, string> columnMapping in ColumnMappings)
                {
                    stringBuilder.Append(columnMapping.Value);
                    stringBuilder.Append(',');
                }
            }
            if (stringBuilder.Length > 0)
            {
                stringBuilder = stringBuilder.Remove(stringBuilder.Length - 1, 1);
            }
            StringBuilder stringBuilder2 = new StringBuilder(300);
            if (updatemapping != null)
            {
                foreach (string item2 in updatemapping)
                {
                    stringBuilder2.Append("des." + item2 + "=src." + item2 + ",");
                }
            }
            else
            {
                foreach (KeyValuePair<string, string> columnMapping2 in ColumnMappings)
                {
                    stringBuilder2.Append("des." + columnMapping2.Value + "=src." + columnMapping2.Value + ",");
                }
            }
            if (stringBuilder2.Length > 0)
            {
                stringBuilder2 = stringBuilder2.Remove(stringBuilder2.Length - 1, 1);
            }
            StringBuilder stringBuilder3 = new StringBuilder();
            foreach (string key in keys)
            {
                stringBuilder3.Append("des." + key + "=src." + key + " AND ");
            }
            if (stringBuilder3.Length > 0)
            {
                stringBuilder3 = stringBuilder3.Remove(stringBuilder3.Length - 4, 4);
            }
            StringBuilder stringBuilder4 = new StringBuilder();
            if (insertmapping != null)
            {
                foreach (string item3 in insertmapping)
                {
                    stringBuilder4.Append("src." + item3 + ",");
                }
            }
            else
            {
                foreach (KeyValuePair<string, string> columnMapping3 in ColumnMappings)
                {
                    stringBuilder4.Append("src." + columnMapping3.Value + ",");
                }
            }
            if (stringBuilder4.Length > 0)
            {
                stringBuilder4 = stringBuilder4.Remove(stringBuilder4.Length - 1, 1);
            }
            dbCommand.CommandText = sqlfunc(targetTable, tempTable, stringBuilder2.ToString(), stringBuilder3.ToString(), stringBuilder.ToString(), stringBuilder4.ToString());
            _log.LogTrace(dbCommand.CommandText);
            await dbCommand.ExecuteNonQueryAsync();
        }

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
        public async Task UpdateToServer<T>(string connection_string, List<string> where_name, List<string> update_name, string targetTable, List<T> list, string tempTable = null)
        {
            using SqlConnection connection = new SqlConnection(connection_string);
            await connection.OpenAsync();
            await UpdateToServer(connection, where_name, update_name, targetTable, list, tempTable);
        }

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
        public async Task UpdateToServer<T>(DbConnection connection, List<string> where_name, List<string> update_name, string targetTable, List<T> list, string tempTable = null, bool createtemp = true)
        {
            if (string.IsNullOrEmpty(tempTable))
            {
                tempTable = "#" + targetTable;
            }
            if (!tempTable.StartsWith("#"))
            {
                tempTable = "#" + tempTable;
            }
            if (createtemp)
            {
                await CreateTempTable(tempTable, targetTable, connection);
            }
            await CopyToServer(connection, tempTable, list);
            DbCommand dbCommand = connection.CreateCommand();
            StringBuilder stringBuilder = new StringBuilder(300);
            foreach (string item in update_name)
            {
                stringBuilder.Append(item + "=" + tempTable + "." + item);
                stringBuilder.Append(',');
            }
            if (stringBuilder.Length > 0)
            {
                stringBuilder = stringBuilder.Remove(stringBuilder.Length - 1, 1);
            }
            StringBuilder stringBuilder2 = new StringBuilder();
            foreach (string item2 in where_name)
            {
                stringBuilder2.Append(targetTable + "." + item2 + "=" + tempTable + "." + item2 + " AND ");
            }
            if (stringBuilder2.Length > 0)
            {
                stringBuilder2 = stringBuilder2.Remove(stringBuilder2.Length - 4, 4);
            }
            LoggerExtensions.LogTrace(message: dbCommand.CommandText = $"UPDATE {targetTable} SET {stringBuilder}\r\n                                  FROM {tempTable}\r\n                               WHERE {stringBuilder2}", logger: _log, args: Array.Empty<object>());
            await dbCommand.ExecuteNonQueryAsync();
        }

        /// <summary>
        /// create temporary table
        /// </summary>
        /// <param name="tempTable">create temporary table name</param>
        /// <param name="targetTable">rarget table name</param>
        /// <param name="connection">database connecting</param>
        public async Task CreateTempTable(string tempTable, string targetTable, DbConnection connection)
        {
            StringBuilder stringBuilder = new StringBuilder(300);
            foreach (KeyValuePair<string, string> columnMapping in ColumnMappings)
            {
                stringBuilder.Append(columnMapping.Value);
                stringBuilder.Append(',');
            }
            if (stringBuilder.Length > 0)
            {
                stringBuilder = stringBuilder.Remove(stringBuilder.Length - 1, 1);
            }
            DbCommand dbCommand = connection.CreateCommand();
            dbCommand.CommandText = $"SELECT {stringBuilder} INTO {tempTable} FROM {targetTable} WHERE 1=2;";
            _log.LogTrace(dbCommand.CommandText);
            await dbCommand.ExecuteNonQueryAsync();
        }
    }
}
