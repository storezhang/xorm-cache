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
        Addr:     addr,
        Password: password,
        DB:       db,
    },
))
```

### 增加过期时间设置
如果不设置过期时间，系统会设置一个默认的过期时间：5min（5分钟）
```go
Engine.SetDefaultCacher(cache.NewRedisCache(
    &redis.Options{
        Addr:     addr,
        Password: password,
        DB:       db,
    },
    cache.WithExpiration(conf.C.Cache.Expiration),
))
```

### 增加缓存前缀
```go
Engine.SetDefaultCacher(cache.NewRedisCache(
    &redis.Options{
        Addr:     addr,
        Password: password,
        DB:       db,
    },
    cache.WithPrefix(prefix),
))
```
