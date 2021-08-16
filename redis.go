package cache

import (
	`context`

	`github.com/go-redis/redis/v8`
	log `github.com/sirupsen/logrus`
	`xorm.io/xorm/caches`
)

var (
	_ caches.Cacher = (*redisCache)(nil)
	_               = NewRedisCache(nil)
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

func (r *redisCache) GetIds(table string, sql string) (value interface{}) {
	sqlKey := r.getSqlKey(table, sql)
	value = r.getObject(sqlKey)

	return
}

func (r *redisCache) GetBean(table string, id string) (value interface{}) {
	beanKey := r.getBeanKey(table, id)
	value = r.getObject(beanKey)

	return
}

func (r *redisCache) PutIds(table string, sql string, ids interface{}) {
	sqlKey := r.getSqlKey(table, sql)
	r.putObject(sqlKey, ids)
}

func (r *redisCache) PutBean(table string, id string, obj interface{}) {
	beanKey := r.getBeanKey(table, id)
	r.putObject(beanKey, obj)
}

func (r *redisCache) DelIds(table string, sql string) {
	if err := r.delObject(r.getSqlKey(table, sql)); nil != err {
		log.WithFields(log.Fields{
			"table": table,
			"sql":   sql,
			"error": err,
		}).Error("删除SQL缓存出错")
	}
}

func (r *redisCache) DelBean(table string, id string) {
	if err := r.delObject(r.getBeanKey(table, id)); nil != err {
		log.WithFields(log.Fields{
			"table": table,
			"id":    id,
			"error": err,
		}).Error("删除对象缓存出错")
	}
}

func (r *redisCache) ClearIds(table string) {
	if err := r.delObjects(r.getAllSqlKey(table)); nil != err {
		log.WithFields(log.Fields{
			"table": table,
			"error": err,
		}).Error("删除数据库表缓存出错")
	}
}

func (r *redisCache) ClearBeans(table string) {
	if err := r.delObjects(r.getAllTableKey(table)); nil != err {
		log.WithFields(log.Fields{
			"table": table,
			"error": err,
		}).Error("删除数据库表的所有对象缓存出错")
	}
}

func (r *redisCache) getObject(key string) (value interface{}) {
	data, err := r.client.Get(context.Background(), key).Bytes()
	if nil != err {
		log.WithFields(log.Fields{
			"key":   key,
			"error": err,
		}).Error("从Redis取缓存出错")
	}

	// 如果没有数据，不用进行下一步
	if 0 == len(data) {
		return
	}

	// 反序列化出真实的数据
	if value, err = r.deserialize(data); nil != err {
		log.WithFields(log.Fields{
			"key":   key,
			"error": err,
		}).Error("反序列化对象出错")
	}

	return
}

func (r *redisCache) putObject(key string, value interface{}) {
	var (
		data []byte
		err  error
	)
	if data, err = r.serialize(value); nil != err {
		log.WithFields(log.Fields{
			"key":   key,
			"value": value,
			"error": err,
		}).Error("序列化对象出错")

		return
	}

	if err := r.client.SetEX(context.Background(), key, data, r.options.Expiration).Err(); nil != err {
		log.WithFields(log.Fields{
			"key":   key,
			"value": value,
			"error": err,
		}).Error("往Redis写入缓存出错")
	}
}

func (r *redisCache) delObject(key string) (err error) {
	if 0 == r.client.Exists(context.Background(), key).Val() {
		err = caches.ErrCacheMiss

		return
	}

	if _, err := r.client.Del(context.Background(), key).Result(); nil != err {
		log.WithFields(log.Fields{
			"key":   key,
			"error": err,
		}).Error("从Redis删除对象出错")
	}

	return
}

func (r *redisCache) delObjects(key string) (err error) {
	var keys []string
	if keys, err = r.client.Keys(context.Background(), key).Result(); nil != err {
		log.WithFields(log.Fields{
			"key":   key,
			"error": err,
		}).Error("从Redis获得所有对象列表的Key出错")

		return
	}

	if 0 == len(keys) {
		return
	}
	if _, err := r.client.Del(context.Background(), keys...).Result(); nil != err {
		log.WithFields(log.Fields{
			"key":   key,
			"error": err,
		}).Error("从Redis上删除对象列表出错")
	}

	return
}
