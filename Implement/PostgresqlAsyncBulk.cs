using FastMember;
using Microsoft.Extensions.Logging;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace MergeServer.Core
{
    /// <summary>
    /// PostgreSql batch operate
    /// </summary>
    public class PostgresqlAsyncBulk : IDbAsyncBulk
    {
        private ILogger _log;

        /// <summary>
        /// database fileds
        /// </summary>
        public List<NpgMappingInfo> Mappings { get; set; }

        /// <summary>
        /// construct function
        /// </summary>
        /// <param name="log">log</param>
        public PostgresqlAsyncBulk(ILogger<PostgresqlAsyncBulk> log)
        {
            _log = log;
        }
        /// <summary>
        /// default init.
        /// use reflect to auto init all type, to lower case database fileds,and  default basic type.
        /// if ignore some fileds,please use DbBulk,Ignore property to remarkable fileds.
        /// if other operating,need user-defined to init operate.
        /// </summary>
        /// <typeparam name="T">Corresponding type</typeparam>
        /// <summary>
        /// 默认初始化。
        /// 使用反射自动初始化所有类型，使用字段名，数据库字段使用字段名小写。数据类型使用默认的基本类型。
        /// 会忽略使用DbBulkIgnore标记的字段。
        /// 若有特殊处理，需要自定义 进行初始化操作。
        /// </summary>
        /// <param name="type"></param>
        public Task InitDefaultMappings<T>()
        {
            PropertyInfo[] properties = typeof(T).GetProperties();
            Mappings = new List<NpgMappingInfo>();
            PropertyInfo[] array = properties;
            foreach (PropertyInfo propertyInfo in array)
            {
                object[] customAttributes = propertyInfo.GetCustomAttributes(typeof(DbBulkAttribute), inherit: false);
                if (customAttributes.Length == 0)
                {
                    Mappings.Add(new NpgMappingInfo
                    {
                        ObjectKey = propertyInfo.Name,
                        DbKey = propertyInfo.Name.ToLower(),
                        Type = propertyInfo.PropertyType.ToNpgsqlType()
                    });
                    continue;
                }
                DbBulkAttribute dbBulkAttribute = customAttributes.First() as DbBulkAttribute;
                if (!dbBulkAttribute.Ignore)
                {
                    string dbKey = (string.IsNullOrEmpty(dbBulkAttribute.ColumnName) ? propertyInfo.Name.ToLower() : dbBulkAttribute.ColumnName);
                    Mappings.Add(new NpgMappingInfo
                    {
                        ObjectKey = propertyInfo.Name,
                        DbKey = dbKey,
                        Type = dbBulkAttribute.Type
                    });
                }
            }
            return Task.CompletedTask;
        }

        /// <summary>
        /// 批量添加
        /// </summary>
        /// <typeparam name="T">待添加的对象实体类型</typeparam>
        /// <param name="connection_string">数据库连接字符串</param>
        /// <param name="targetTable">目标表名</param>
        /// <param name="list">待添加的数据列表</param>
        public async Task CopyToServer<T>(string connection_string, string targetTable, List<T> list)
        {
            using NpgsqlConnection connection = new NpgsqlConnection(connection_string);
            await connection.OpenAsync();
            await CopyToServer(connection, targetTable, list);
        }

        /// <summary>
        /// 批量添加
        /// </summary>
        /// <typeparam name="T">待添加的对象实体类型</typeparam>
        /// <param name="connection">数据库连接，需要确保数据库连接已经打开。若无后续操作，不应复用该连接。</param>
        /// <param name="targetTable">目标表名</param>
        /// <param name="list">待添加的数据列表</param>
        public async Task CopyToServer<T>(DbConnection connection, string targetTable, List<T> list)
        {
            StringBuilder stringBuilder = new StringBuilder(100);
            foreach (NpgMappingInfo mapping in Mappings)
            {
                stringBuilder.Append(mapping.DbKey);
                stringBuilder.Append(',');
            }
            string text = stringBuilder.Remove(stringBuilder.Length - 1, 1).ToString();
            string text2 = "COPY " + targetTable + " (" + text + ") FROM STDIN (FORMAT BINARY)";
            _log.LogTrace(text2);
            using ObjectReader reader = ObjectReader.Create(list, Mappings.Select((NpgMappingInfo e) => e.ObjectKey).ToArray());
            NpgsqlConnection npgsqlConnection = connection as NpgsqlConnection;
            using NpgsqlBinaryImporter writer = npgsqlConnection.BeginBinaryImport(text2);
            while (reader.Read())
            {
                await writer.StartRowAsync();
                foreach (NpgMappingInfo mapping2 in Mappings)
                {
                    await writer.WriteAsync(reader[mapping2.ObjectKey], mapping2.Type);
                }
            }
            await writer.CompleteAsync();
        }

        /// <summary>
        /// 存在则更新，不存在则插入表。
        /// 1.新建临时表
        /// 2.数据导入临时表
        /// 3.数据merge到目标表
        /// </summary>
        /// <typeparam name="T">数据类型</typeparam>
        /// <param name="connection_string">连接字符串</param>
        /// <param name="keys">源表与目标表进行匹配的字段名，需要主键或唯一。否则可能会导致错误。</param>
        /// <param name="targetTable">目标表</param>
        /// <param name="list">待导入的数据列表</param>
        /// <param name="tempTable">导入到临时表表名, 默认为目标表名+#或+_temp</param>
        /// <param name="insertmapping">需要插入的列，为空则直接使用Mapping的字段，为了避免插入自增列</param>
        /// <param name="updatemapping">需要更新的列，为空则直接使用Mapping的字段</param>
        public async Task MergeToServer<T>(string connection_string, List<string> keys, string targetTable, List<T> list, string tempTable = null, List<string> insertmapping = null, List<string> updatemapping = null)
        {
            using NpgsqlConnection connection = new NpgsqlConnection(connection_string);
            await connection.OpenAsync();
            await MergeToServer(connection, keys, targetTable, list, tempTable, insertmapping, updatemapping);
        }

        /// <summary>
        /// 存在则更新，不存在则插入表。
        /// 1.新建临时表
        /// 2.数据导入临时表
        /// 3.数据merge到目标表
        /// </summary>
        /// <typeparam name="T">数据类型</typeparam>
        /// <param name="connection">数据库连接，需要确保数据库连接已经打开。若无后续操作，不应复用该连接。</param>
        /// <param name="keys">源表与目标表进行匹配的字段名，需要主键或唯一。否则可能会导致错误。</param>
        /// <param name="targetTable">目标表</param>
        /// <param name="list">待导入的数据列表</param>
        /// <param name="tempTable">导入到临时表表名, 默认为目标表名+#或+_temp</param>
        /// <param name="insertmapping">需要插入的列，为空则直接使用Mapping的字段，为了避免插入自增列</param>
        /// <param name="updatemapping">需要更新的列，为空则直接使用Mapping的字段</param>
        public async Task MergeToServer<T>(DbConnection connection, List<string> keys, string targetTable, List<T> list, string tempTable = null, List<string> insertmapping = null, List<string> updatemapping = null)
        {
            if (string.IsNullOrEmpty(tempTable))
            {
                tempTable = targetTable + "_temp";
            }
            await ImportToTemp(connection, tempTable, targetTable, list);
            await Merge(connection, keys, targetTable, list, tempTable, insertmapping, updatemapping);
        }

        private async Task Merge<T>(DbConnection connection, List<string> keys, string targetTable, List<T> list, string tempTable, List<string> insertmapping, List<string> updatemapping)
        {
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
                foreach (NpgMappingInfo mapping in Mappings)
                {
                    stringBuilder.Append(mapping.DbKey);
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
                    stringBuilder2.Append(item2 + "=EXCLUDED." + item2 + ",");
                }
            }
            else
            {
                foreach (NpgMappingInfo mapping2 in Mappings)
                {
                    stringBuilder2.Append(mapping2.DbKey + "=EXCLUDED." + mapping2.DbKey + ",");
                }
            }
            if (stringBuilder2.Length > 0)
            {
                stringBuilder2 = stringBuilder2.Remove(stringBuilder2.Length - 1, 1);
            }
            StringBuilder stringBuilder3 = new StringBuilder();
            foreach (string key in keys)
            {
                stringBuilder3.Append(key + ",");
            }
            if (stringBuilder3.Length > 0)
            {
                stringBuilder3 = stringBuilder3.Remove(stringBuilder3.Length - 1, 1);
            }
            LoggerExtensions.LogTrace(message: dbCommand.CommandText = $"INSERT INTO {targetTable} ({stringBuilder})\r\n                                  SELECT {stringBuilder} FROM  {tempTable}\r\n                                    ON CONFLICT({stringBuilder3}) DO UPDATE SET {stringBuilder2}", logger: _log, args: Array.Empty<object>());
            await dbCommand.ExecuteNonQueryAsync();
        }

        private async Task ImportToTemp<T>(DbConnection connection, string tempTable, string targetTable, List<T> list)
        {
            await CreateTempTable(tempTable, targetTable, connection);
            await CopyToServer(connection, tempTable, list);
        }

        /// <summary>
        /// 批量更新操作。
        /// 1.新建临时表
        /// 2.数据导入临时表
        /// 3.数据更新到目标表
        /// </summary>
        /// <typeparam name="T">数据类型</typeparam>
        /// <param name="connection_string">连接字符串</param>
        /// <param name="where_name">匹配where比较的字段</param>
        /// <param name="update_name">需要更新的字段名</param>
        /// <param name="targetTable">目标表</param>
        /// <param name="list">待导入的数据列表</param>
        /// <param name="tempTable">导入到临时表表名, 默认为目标表名+#或+_temp</param>
        public async Task UpdateToServer<T>(string connection_string, List<string> where_name, List<string> update_name, string targetTable, List<T> list, string tempTable = null)
        {
            using NpgsqlConnection connection = new NpgsqlConnection(connection_string);
            await connection.OpenAsync();
            await UpdateToServer(connection, where_name, update_name, targetTable, list, tempTable);
        }

        /// <summary>
        /// 批量更新操作。
        /// 1.新建临时表
        /// 2.数据导入临时表
        /// 3.数据更新到目标表
        /// </summary>
        /// <typeparam name="T">数据类型</typeparam>
        /// <param name="connection">数据库连接，需要确保数据库连接已经打开。若无后续操作，不应复用该连接。</param>
        /// <param name="where_name">匹配where比较的字段</param>
        /// <param name="update_name">需要更新的字段名</param>
        /// <param name="targetTable">目标表</param>
        /// <param name="list">待导入的数据列表</param>
        /// <param name="tempTable">导入到临时表表名, 默认为目标表名+#或+_temp</param>
        /// <param name="createtemp">是否创建临时表</param>
        public async Task UpdateToServer<T>(DbConnection connection, List<string> where_name, List<string> update_name, string targetTable, List<T> list, string tempTable = null, bool createtemp = true)
        {
            if (string.IsNullOrEmpty(tempTable))
            {
                tempTable = targetTable + "_temp";
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
        /// 存在则更新，不存在则插入表，源表不存在而目标表存在则删除。默认带事务。
        /// 1.新建临时表
        /// 2.数据导入临时表
        /// 3.将临时表不存在，但目标表存在的数据删除
        /// 4.数据merge到目标表
        /// </summary>
        /// <typeparam name="T">数据类型</typeparam>
        /// <param name="connection_string">连接字符串</param>
        /// <param name="keys">源表与目标表进行匹配的字段名，需要主键或唯一。否则可能会导致错误。</param>
        /// <param name="targetTable">目标表</param>
        /// <param name="list">待导入的数据列表</param>
        /// <param name="tempTable">导入到临时表表名, 默认为目标表名+#或+_temp</param>
        /// <param name="insertmapping">需要插入的列，为空则直接使用Mapping的字段，为了避免插入自增列</param>
        /// <param name="updatemapping">需要更新的列，为空则直接使用Mapping的字段</param>
        public async Task MergeAndDeleteToServer<T>(string connection_string, List<string> keys, string targetTable, List<T> list, string tempTable = null, List<string> insertmapping = null, List<string> updatemapping = null)
        {
            using NpgsqlConnection connection = new NpgsqlConnection(connection_string);
            await connection.OpenAsync();
            if (string.IsNullOrEmpty(tempTable))
            {
                tempTable = targetTable + "_temp";
            }
            await ImportToTemp(connection, tempTable, targetTable, list);
            using NpgsqlTransaction trans = await connection.BeginTransactionAsync();
            await MergeAndDeleteToServer(connection, keys, targetTable, list, tempTable, insertmapping, updatemapping);
            await trans.CommitAsync();
        }

        /// <summary>
        /// 存在则更新，不存在则插入表，源表不存在而目标表存在则删除。默认不带事务，需要手动设置事务。
        /// 1.新建临时表
        /// 2.数据导入临时表
        /// 3.将临时表不存在，但目标表存在的数据删除
        /// 4.数据merge到目标表
        /// </summary>
        /// <typeparam name="T">数据类型</typeparam>
        /// <param name="connection">数据库连接，需要确保数据库连接已经打开。若无后续操作，不应复用该连接。</param>
        /// <param name="keys">源表与目标表进行匹配的字段名，需要主键或唯一。否则可能会导致错误。</param>
        /// <param name="targetTable">目标表</param>
        /// <param name="list">待导入的数据列表</param>
        /// <param name="tempTable">导入到临时表表名, 默认为目标表名+#或+_temp</param>
        /// <param name="insertmapping">需要插入的列，为空则直接使用Mapping的字段，为了避免插入自增列</param>
        /// <param name="updatemapping">需要更新的列，为空则直接使用Mapping的字段</param>
        public async Task MergeAndDeleteToServer<T>(DbConnection connection, List<string> keys, string targetTable, List<T> list, string tempTable = null, List<string> insertmapping = null, List<string> updatemapping = null)
        {
            StringBuilder stringBuilder = new StringBuilder(100);
            foreach (string key in keys)
            {
                stringBuilder.Append(" " + targetTable + "." + key + "=" + tempTable + "." + key + " AND ");
            }
            if (stringBuilder.Length > 0)
            {
                stringBuilder = stringBuilder.Remove(stringBuilder.Length - 4, 4);
            }
            string text = $"DELETE FROM {targetTable} WHERE NOT EXISTS(SELECT * FROM {tempTable} WHERE {stringBuilder})";
            DbCommand dbCommand = connection.CreateCommand();
            dbCommand.CommandText = text;
            _log.LogTrace(text);
            await dbCommand.ExecuteNonQueryAsync();
            await Merge(connection, keys, targetTable, list, tempTable, insertmapping, updatemapping);
        }

        /// <summary>
        /// 创建临时表
        /// </summary>
        /// <param name="tempTable">临时表名</param>
        /// <param name="targetTable">目标表名</param>
        /// <param name="connection">数据库连接</param>
        public async Task CreateTempTable(string tempTable, string targetTable, DbConnection connection)
        {
            DbCommand dbCommand = connection.CreateCommand();
            dbCommand.CommandText = "CREATE TEMP TABLE " + tempTable + " (LIKE " + targetTable + ")";
            _log.LogTrace(dbCommand.CommandText);
            await dbCommand.ExecuteNonQueryAsync();
        }
    }
}
