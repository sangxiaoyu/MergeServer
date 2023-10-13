
# MergeServer Function
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


That's all.If you have any questions, please commit an issue.
