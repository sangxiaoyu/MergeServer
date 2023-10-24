
# MergeServer 
This is a sql batch fucntion,help  synchronize data in batches, thereby reducing losses.

Now,we can support SQL Server batch operating,next time we will open PostgreSQL ,MySQL,and other ...

how to use it?

you can register it to startup or instantiate service.

such as :

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
              education = "本科",
              email = "mock@163.com",
              name = $"小榆{i}",
              nation = "汉",
              nationality="中国"
          });
      }
      await _bulk.CopyToServer(connectStr, "user_base", mock_list);
  }
```

```C#
public async Task MergeToServerTest()
  {
      var connectStr = @"Data Source=KF009\SQLEXPRESS;Initial Catalog=MockData;User ID=sa;Password=root";
      await _bulk.InitDefaultMappings<UserBaseModel>();
      var mock_list = new List<UserBaseModel>();
      for (var i = 0; i < 1000; i++)
      {
          mock_list.Add(new UserBaseModel
          {
              age = i,
              birthday = DateTime.Now.AddMonths(-i).Date,
              education = "本科",
              email = "mock@163.com",
              name = $"小榆{i}",
              nation = "汉",
              nationality = "中国"
          });
      }
      var insertMapping = new List<string> { "birthday", "education", "age", "email", "name", "nation", "nationality" };
      var updateMapping = new List<string> { "birthday", "education", "age", "email"};
      await _bulk.MergeToServer(connectStr,new List<string> {"id"}, "user_base", mock_list,null, insertMapping, updateMapping);
  }
```

That's all.If you have any questions, please commit an issue.
