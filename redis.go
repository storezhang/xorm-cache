package cache

import (
	`bytes`
	`context`
	`encoding/gob`
	`fmt`
	`hash/crc32`
	`reflect`
	`time`
	`unsafe`

	`github.com/go-redis/redis/v8`
)

const (
	beanPrefix = "bean"
	sqlPrefix  = "sql"
)

type redisCache struct {
	client     *redis.Client
	expiration time.Duration
}

// NewRedisCache 创建一个基于Redis存储的分布式缓存
func NewRedisCache(redisOptions *redis.Options, options ...option) *redisCache {
	appliedOptions := defaultOptions()
	for _, option := range options {
		option.apply(&appliedOptions)
	}

	return &redisCache{
		client:     redis.NewClient(redisOptions),
		expiration: appliedOptions.Expiration,
	}
}

func (c *redisCache) getBeanKey(table string, id string) string {
	return fmt.Sprintf("%s:%s:%s", beanPrefix, table, id)
}

func (c *redisCache) getSqlKey(table string, sql string) string {
	return fmt.Sprintf("%s:%s:%d", sqlPrefix, table, crc32.ChecksumIEEE([]byte(sql)))
}

func (c *redisCache) getObject(key string) (value interface{}) {
	data, err := c.client.Get(context.Background(), key).Bytes()
	if nil != err {
		return
	}
	value, err = c.deserialize(data)

	return
}

func (c *redisCache) GetIds(table string, sql string) (value interface{}) {
	sqlKey := c.getSqlKey(table, sql)
	value = c.getObject(sqlKey)

	return
}

func (c *redisCache) GetBean(table string, id string) (value interface{}) {
	beanKey := c.getBeanKey(table, id)
	value = c.getObject(beanKey)

	return
}

func (c *redisCache) putObject(key string, value interface{}) {
	var (
		data []byte
		err  error
	)
	if data, err = c.serialize(value); nil != err {
		return
	}

	if 0 == c.expiration {
		c.expiration = time.Duration(-1)
	}
	_, _ = c.client.SetEX(context.Background(), key, data, c.expiration).Result()
}

func (c *redisCache) PutIds(table, sql string, ids interface{}) {
	sqlKey := c.getSqlKey(table, sql)
	c.putObject(sqlKey, ids)
}

func (c *redisCache) PutBean(table string, id string, obj interface{}) {
	beanKey := c.getBeanKey(table, id)
	c.putObject(beanKey, obj)
}

func (c *redisCache) delObject(key string) (err error) {
	_, err = c.client.Del(context.Background(), key).Result()

	return
}

func (c *redisCache) delObjects(key string) (err error) {
	var keys []string
	if keys, err = c.client.Keys(context.Background(), key).Result(); nil != err {
		return
	}
	c.client.Del(context.Background(), keys...)

	return
}

func (c *redisCache) DelIds(table string, sql string) {
	_ = c.delObject(c.getSqlKey(table, sql))
}

func (c *redisCache) DelBean(table string, id string) {
	_ = c.delObject(c.getBeanKey(table, id))
}

func (c *redisCache) ClearIds(table string) {
	_ = c.delObjects(fmt.Sprintf("%s:%s:*", sqlPrefix, table))
}

func (c *redisCache) ClearBeans(table string) {
	_ = c.delObjects(c.getBeanKey(table, "*"))
}

func (c *redisCache) serialize(obj interface{}) (data []byte, err error) {
	c.registerGobConcreteType(obj)

	if reflect.Struct == reflect.TypeOf(obj).Kind() {
		err = fmt.Errorf("序列化只支持Struct指针")

		return
	}

	var buffer bytes.Buffer
	encoder := gob.NewEncoder(&buffer)

	if err = encoder.Encode(&obj); nil != err {
		return
	}
	data = buffer.Bytes()

	return
}

func (c *redisCache) deserialize(data []byte) (ptr interface{}, err error) {
	buffer := bytes.NewBuffer(data)
	decoder := gob.NewDecoder(buffer)

	var obj interface{}
	if err = decoder.Decode(&obj); nil != err {
		return
	}

	value := reflect.ValueOf(obj)
	if value.Kind() == reflect.Struct {
		var objPtr interface{} = &obj
		interfaceData := reflect.ValueOf(objPtr).Elem().InterfaceData()
		sp := reflect.NewAt(value.Type(), unsafe.Pointer(interfaceData[1])).Interface()
		ptr = sp
	} else {
		ptr = obj
	}

	return
}

func (c *redisCache) registerGobConcreteType(obj interface{}) {
	typeOf := reflect.TypeOf(obj)

	switch typeOf.Kind() {
	case reflect.Ptr:
		value := reflect.ValueOf(obj)
		i := value.Elem().Interface()
		gob.Register(&i)
	case reflect.Struct, reflect.Map, reflect.Slice:
		gob.Register(obj)
	case reflect.String:
		fallthrough
	case reflect.Int8, reflect.Uint8:
		fallthrough
	case reflect.Int16, reflect.Uint16:
		fallthrough
	case reflect.Int32, reflect.Uint32:
		fallthrough
	case reflect.Int, reflect.Uint:
		fallthrough
	case reflect.Int64, reflect.Uint64:
		fallthrough
	case reflect.Bool:
		fallthrough
	case reflect.Float32, reflect.Float64:
		fallthrough
	case reflect.Complex64, reflect.Complex128:
		// do nothing since already registered known type
	}

	return
}
