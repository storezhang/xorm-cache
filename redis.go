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

func (rc *redisCache) PutIds(table string, sql string, ids interface{}) {
	sqlKey := rc.getSqlKey(table, sql)
	rc.putObject(sqlKey, ids)
}

func (rc *redisCache) PutBean(table string, id string, obj interface{}) {
	beanKey := rc.getBeanKey(table, id)
	rc.putObject(beanKey, obj)
}

func (rc *redisCache) DelIds(table string, sql string) {
	if err := rc.delObject(rc.getSqlKey(table, sql)); nil != err {
		log.WithFields(log.Fields{
			"table": table,
			"sql":   sql,
			"error": err,
		}).Error("删除SQL缓存出错")
	}
}

func (rc *redisCache) DelBean(table string, id string) {
	if err := rc.delObject(rc.getBeanKey(table, id)); nil != err {
		log.WithFields(log.Fields{
			"table": table,
			"id":    id,
			"error": err,
		}).Error("删除对象缓存出错")
	}
}

func (rc *redisCache) ClearIds(table string) {
	if err := rc.delObjects(rc.getTableKey(table)); nil != err {
		log.WithFields(log.Fields{
			"table": table,
			"error": err,
		}).Error("删除数据库表缓存出错")
	}
}

func (rc *redisCache) ClearBeans(table string) {
	if err := rc.delObjects(rc.getBeanKey(table, "*")); nil != err {
		log.WithFields(log.Fields{
			"table": table,
			"error": err,
		}).Error("删除数据库表的所有对象缓存出错")
	}
}

func (rc *redisCache) getObject(key string) (value interface{}) {
	data, err := rc.client.Get(context.Background(), key).Bytes()
	if nil != err {
		log.WithFields(log.Fields{
			"key":   key,
			"error": err,
		}).Error("从Redis取缓存出错")

		return
	}
	if value, err = rc.deserialize(data); nil != err {
		log.WithFields(log.Fields{
			"key":   key,
			"error": err,
		}).Error("反序列化对象出错")
	}

	return
}

func (rc *redisCache) putObject(key string, value interface{}) {
	var (
		data []byte
		err  error
	)
	if data, err = rc.serialize(value); nil != err {
		log.WithFields(log.Fields{
			"key":   key,
			"value": value,
			"error": err,
		}).Error("序列化对象出错")

		return
	}

	if _, err := rc.client.SetEX(context.Background(), key, data, rc.options.Expiration).Result(); nil != err {
		log.WithFields(log.Fields{
			"key":   key,
			"value": value,
			"error": err,
		}).Error("往Redis写入缓存出错")
	}
}

func (rc *redisCache) delObject(key string) (err error) {
	if _, err := rc.client.Del(context.Background(), key).Result(); nil != err {
		log.WithFields(log.Fields{
			"key":   key,
			"error": err,
		}).Error("从Redis删除对象出错")
	}

	return
}

func (rc *redisCache) delObjects(key string) (err error) {
	var keys []string
	if keys, err = rc.client.Keys(context.Background(), key).Result(); nil != err {
		log.WithFields(log.Fields{
			"key":   key,
			"error": err,
		}).Error("从Redis获得所有对象列表的Key出错")

		return
	}
	if _, err := rc.client.Del(context.Background(), keys...).Result(); nil != err {
		log.WithFields(log.Fields{
			"key":   key,
			"error": err,
		}).Error("从Redis上删除对象列表出错")
	}

	return
}
