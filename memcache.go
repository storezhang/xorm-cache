package cache

import (
	`github.com/bradfitz/gomemcache/memcache`
	`xorm.io/xorm/caches`
)

var (
	_ caches.Cacher = (*memcacheCache)(nil)
	_               = NewMemcacheCache(nil)
)

type memcacheCache struct {
	baseDistributedCache

	client *memcache.Client
}

// NewMemcacheCache 创建一个基于Memcache存储的分布式缓存
func NewMemcacheCache(client *memcache.Client, options ...option) *memcacheCache {
	appliedOptions := defaultOptions()
	for _, option := range options {
		option.apply(&appliedOptions)
	}

	return &memcacheCache{
		client: client,
		baseDistributedCache: baseDistributedCache{
			baseCache: baseCache{
				options: appliedOptions,
			},
		},
	}
}

func (mc *memcacheCache) GetIds(table string, sql string) (value interface{}) {
	sqlKey := mc.getSqlKey(table, sql)
	value = mc.getObject(sqlKey)

	return
}

func (mc *memcacheCache) GetBean(table string, id string) (value interface{}) {
	beanKey := mc.getBeanKey(table, id)
	value = mc.getObject(beanKey)

	return
}

func (mc *memcacheCache) PutIds(table string, sql string, ids interface{}) {
	sqlKey := mc.getSqlKey(table, sql)
	mc.putObject(sqlKey, ids)
}

func (mc *memcacheCache) PutBean(table string, id string, obj interface{}) {
	beanKey := mc.getBeanKey(table, id)
	mc.putObject(beanKey, obj)
}

func (mc *memcacheCache) DelIds(table string, sql string) {
	_ = mc.delObject(mc.getSqlKey(table, sql))
}

func (mc *memcacheCache) DelBean(table string, id string) {
	_ = mc.delObject(mc.getBeanKey(table, id))
}

func (mc *memcacheCache) ClearIds(table string) {
	_ = mc.delObjects(mc.getTableKey(table))
}

func (mc *memcacheCache) ClearBeans(table string) {
	_ = mc.delObjects(mc.getBeanKey(table, "*"))
}

func (mc *memcacheCache) getObject(key string) (value interface{}) {
	var (
		item *memcache.Item
		err  error
	)
	if item, err = mc.client.Get(key); nil != err {
		return
	}
	value, err = mc.deserialize(item.Value)

	return
}

func (mc *memcacheCache) putObject(key string, value interface{}) {
	var (
		data []byte
		err  error
	)
	if data, err = mc.serialize(value); nil != err {
		return
	}

	_ = mc.client.Set(&memcache.Item{
		Key:        key,
		Value:      data,
		Flags:      0,
		Expiration: int32(mc.options.Expiration.Seconds()),
	})
}

func (mc *memcacheCache) delObject(key string) (err error) {
	return mc.client.Delete(key)
}

func (mc *memcacheCache) delObjects(keys ...string) (err error) {
	for _, key := range keys {
		err = mc.client.Delete(key)
	}

	return
}
