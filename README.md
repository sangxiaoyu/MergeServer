# MergeServer

This is a sql batch fucntion,help synchronize data in batches, thereby reducing losses.

Now,we can support SQL Server batch operating,next time we will open PostgreSQL ,MySQL,and other ...

how to use it?

you can register it to startup or instantiate service.

if you use SQL Server,such as :

```C#
//if you use SqlServer database, config SqlServerAsyncBulk service.
services.AddSingleton<IDbAsyncBulk, SqlServerAsyncBulk>();
```

and then,to instantiate service by dependency injection.

```C#
public class BatchOperate
{
  private readonly IDbAsyncBulk _bulk;
  public BatchOperate(IDbAsyncBulk bulk)
  {
    _bulk = bulk;
  }
}

```

```C#
public async Task CopyToServerTest()
  {
      var connectStr = @"Data Source=KF009\SQLEXPRESS;Initial Catalog=MockData;User ID=sa;Password=root";
      await _bulk.InitDefaultMappings<UserBaseModel>();
      var mock_list = new List<UserBaseModel>();
      for (var i = 0; i < 1000; i++) {
          mock_list.Add(new UserBaseModel
          {
              age = i,
              birthday = DateTime.Now.AddMonths(-i).Date,
              education = "bachelor",
              email = "mock@163.com",
              name = $"xiaoyu{i}",
              nation = "Han nationality",
              nationality="China"
          });
      }
      await _bulk.CopyToServer(connectStr, "user_base", mock_list);
      var insertMapping = new List<string> { "birthday", "education", "age", "email", "name", "nation", "nationality" };
      var updateMapping = new List<string> { "birthday", "education", "age", "email"};
      await _bulk.MergeToServer(connectStr,new List<string> {"id"}, "user_base", mock_list,null, insertMapping, updateMapping);
  }
```

if you use PostgreSQL,such as :

```C#
//if you use PostgreSQL database, config PostgresqlAsyncBulk service.
services.AddTransient<IDbAsyncBulk, PostgresqlAsyncBulk>();
```

and then,to instantiate service by dependency injection.

```C#
private readonly IDbAsyncBulk _bulk;
public MergeUnitController(IDbAsyncBulk bulk)
{
    _bulk = bulk;
}
```

```C#
public async Task<IActionResult> CopyToServer()
{
    await _bulk.InitDefaultMappings<UserInfo>();
    var data = new List<UserInfo>();
    for (int i = 1; i < 100000; i++)
    {
        data.Add(new UserInfo
        {
            id = i,
            address = $"maple load {i}",
            age = i,
            name = $"test number{i}",
            hobby = new string[] { "reading","dancing" }
        });
    }
    //CopyToServer test
    await _bulk.CopyToServer(connection,"public.user_info", data);
    //MergeToServer test
    await _bulk.MergeToServer(connection,new List<string> {"id" },"user_info",data,null);
    return Ok();
}

```

for PostgreSQL,some fileds type need to mapping,some pgsql data type, text[],integer[] and other.

we could remark it by attribute,such as:

```C#
[DbBulk(Type = NpgsqlTypes.NpgsqlDbType.Array | NpgsqlTypes.NpgsqlDbType.Text)]
public string[] hobby { get; set; }
[DbBulk(Type = NpgsqlTypes.NpgsqlDbType.Array | NpgsqlTypes.NpgsqlDbType.Integer)]
public int[] courses { get; set; }
```

test case:
We use **100,000 data**, and the time consumed for batch insert is about **751 ms**, and the time consumed for new modification is **2851 ms** for the same amount of data.

That's all.If you have any questions, please commit an issue.
