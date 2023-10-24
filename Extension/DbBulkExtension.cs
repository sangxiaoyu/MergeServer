using CsvHelper;
using CsvHelper.Configuration;
using NpgsqlTypes;
using System;
using System.Collections.Generic;
using System.Formats.Asn1;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MergeServer.Core
{
    public static class DbBulkExtension
    {
        private static Dictionary<Type, NpgsqlDbType> _postgresqldic = new Dictionary<Type, NpgsqlDbType>
    {
        {
            typeof(int),
            NpgsqlDbType.Integer
        },
        {
            typeof(string),
            NpgsqlDbType.Text
        },
        {
            typeof(DateTime),
            NpgsqlDbType.Timestamp
        },
        {
            typeof(double),
            NpgsqlDbType.Double
        },
        {
            typeof(long),
            NpgsqlDbType.Bigint
        },
        {
            typeof(int?),
            NpgsqlDbType.Integer
        },
        {
            typeof(DateTime?),
            NpgsqlDbType.Timestamp
        },
        {
            typeof(double?),
            NpgsqlDbType.Double
        },
        {
            typeof(long?),
            NpgsqlDbType.Bigint
        },
        {
            typeof(decimal?),
            NpgsqlDbType.Numeric
        },
        {
            typeof(decimal),
            NpgsqlDbType.Numeric
        }
    };

        /// <summary>
        /// 根据类型转换为数据库类型。无配置的类型默认转为字符串。
        /// 转换关系：
        /// { typeof(int),  NpgsqlDbType.Integer},
        /// { typeof(string),  NpgsqlDbType.Text},
        /// { typeof(DateTime),  NpgsqlDbType.Timestamp},
        /// { typeof(double),  NpgsqlDbType.Double},
        /// { typeof(long),  NpgsqlDbType.Bigint},
        /// { typeof(int?),  NpgsqlDbType.Integer},
        /// { typeof(string),  NpgsqlDbType.Text},
        /// { typeof(DateTime?),  NpgsqlDbType.Timestamp},
        /// { typeof(double?),  NpgsqlDbType.Double},
        /// { typeof(long?),  NpgsqlDbType.Bigint},
        /// </summary>
        /// <param name="type">.net类型</param>
        /// <returns></returns>
        public static NpgsqlDbType ToNpgsqlType(this Type type)
        {
            if (_postgresqldic.TryGetValue(type, out var value))
            {
                return value;
            }
            return NpgsqlDbType.Text;
        }

        /// <summary>
        /// 将list数据写入csv文件
        /// </summary>
        /// <typeparam name="T">实体类型</typeparam>
        /// <param name="list">集合</param>
        /// <returns>文件路径</returns>
        public static async Task<string> ToCsvFile<T>(this List<T> list, Action<CsvContext> register_action, string tempdirectory = "tempfile")
        {
            string text = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, tempdirectory);
            if (!Directory.Exists(text))
            {
                Directory.CreateDirectory(text);
            }
            string path = Path.Combine(text, DateTime.Now.ToString("yyyyMMddhhmmss") + ".csv");
            CsvConfiguration configuration = new CsvConfiguration(CultureInfo.InvariantCulture)
            {
                PrepareHeaderForMatch = (PrepareHeaderForMatchArgs args) => args.Header.ToLower(),
                HasHeaderRecord = false,
                NewLine = Environment.NewLine
            };
            using (StreamWriter writer = new StreamWriter(path))
            {
                using CsvWriter csv = new CsvWriter(writer, configuration);
                register_action(csv.Context);
                await csv.WriteRecordsAsync(list);
                await csv.FlushAsync();
            }
            return path;
        }
    }
}
