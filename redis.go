package cache

import (
	`context`

	`github.com/go-redis/redis/v8`
)

type redisCache struct {
	baseDistributedCache

	client *redis.Client
}

// NewRedisCache 创建一个基于Redis存储的分布式缓存
func NewRedisCache(client *redis.Client, options ...option) *redisCache {
	appliedOptions := defaultOptions()
	for _, option := range options {
		option.apply(&appliedOptions)
	}

	return &redisCache{
		client: client,
		baseDistributedCache: baseDistributedCache{
			baseCache: baseCache{
				options: appliedOptions,
			},
		},
	}
}

func (rc *redisCache) GetIds(table string, sql string) (value interface{}) {
	sqlKey := rc.getSqlKey(table, sql)
	value = rc.getObject(sqlKey)

	return
}

func (rc *redisCache) GetBean(table string, id string) (value interface{}) {
	beanKey := rc.getBeanKey(table, id)
	value = rc.getObject(beanKey)

	return
}

func (rc *redisCache) PutIds(table, sql string, ids interface{}) {
	sqlKey := rc.getSqlKey(table, sql)
	rc.putObject(sqlKey, ids)
}

func (rc *redisCache) PutBean(table string, id string, obj interface{}) {
	beanKey := rc.getBeanKey(table, id)
	rc.putObject(beanKey, obj)
}

func (rc *redisCache) DelIds(table string, sql string) {
	_ = rc.delObject(rc.getSqlKey(table, sql))
}

func (rc *redisCache) DelBean(table string, id string) {
	_ = rc.delObject(rc.getBeanKey(table, id))
}

func (rc *redisCache) ClearIds(table string) {
	_ = rc.delObjects(rc.getTableKey(table))
}

func (rc *redisCache) ClearBeans(table string) {
	_ = rc.delObjects(rc.getBeanKey(table, "*"))
}

func (rc *redisCache) getObject(key string) (value interface{}) {
	data, err := rc.client.Get(context.Background(), key).Bytes()
	if nil != err {
		return
	}
	value, err = rc.deserialize(data)

	return
}

func (rc *redisCache) putObject(key string, value interface{}) {
	var (
		data []byte
		err  error
	)
	if data, err = rc.serialize(value); nil != err {
		return
	}

	_, _ = rc.client.SetEX(context.Background(), key, data, rc.options.Expiration).Result()
}

func (rc *redisCache) delObject(key string) (err error) {
	_, err = rc.client.Del(context.Background(), key).Result()

	return
}

func (rc *redisCache) delObjects(key string) (err error) {
	var keys []string
	if keys, err = rc.client.Keys(context.Background(), key).Result(); nil != err {
		return
	}
	rc.client.Del(context.Background(), keys...)

	return
}
