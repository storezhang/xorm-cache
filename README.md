# xorm-cache
XORM的缓存


## 内置
- Redis
- Memcache


## 样例代码

### 使用Redis
```go
Engine.SetDefaultCacher(cache.NewRedisCache(
    &redis.Options{
        Addr:               conf.C.Cache.Redis.Addr,
        Password:           conf.C.Cache.Redis.Password,
        DB:                 conf.C.Cache.Redis.DB,
    },
))
```
