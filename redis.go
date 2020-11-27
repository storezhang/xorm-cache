package cache

import (
	`bytes`
	`context`
	`encoding/gob`
	`fmt`
	`hash/crc32`
	`reflect`
	`unsafe`

	`github.com/go-redis/redis/v8`
)

type redisCache struct {
	client  *redis.Client
	options options
}

// NewRedisCache 创建一个基于Redis存储的分布式缓存
func NewRedisCache(redisOptions *redis.Options, options ...option) *redisCache {
	appliedOptions := defaultOptions()
	for _, option := range options {
		option.apply(&appliedOptions)
	}

	return &redisCache{
		client:  redis.NewClient(redisOptions),
		options: appliedOptions,
	}
}

func (rc *redisCache) getBeanKey(table string, id string) (key string) {
	if "" != rc.options.Prefix {
		key = fmt.Sprintf("%s:%s:%s:%s", beanPrefix, rc.options.Prefix, table, id)
	} else {
		key = fmt.Sprintf("%s:%s:%s", beanPrefix, table, id)
	}

	return
}

func (rc *redisCache) getSqlKey(table string, sql string) (key string) {
	if "" != rc.options.Prefix {
		key = fmt.Sprintf("%s:%s:%s:%d", sqlPrefix, rc.options.Prefix, table, crc32.ChecksumIEEE([]byte(sql)))
	} else {
		key = fmt.Sprintf("%s:%s:%d", sqlPrefix, table, crc32.ChecksumIEEE([]byte(sql)))
	}

	return
}

func (rc *redisCache) getObject(key string) (value interface{}) {
	data, err := rc.client.Get(context.Background(), key).Bytes()
	if nil != err {
		return
	}
	value, err = rc.deserialize(data)

	return
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

func (rc *redisCache) PutIds(table, sql string, ids interface{}) {
	sqlKey := rc.getSqlKey(table, sql)
	rc.putObject(sqlKey, ids)
}

func (rc *redisCache) PutBean(table string, id string, obj interface{}) {
	beanKey := rc.getBeanKey(table, id)
	rc.putObject(beanKey, obj)
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

func (rc *redisCache) DelIds(table string, sql string) {
	_ = rc.delObject(rc.getSqlKey(table, sql))
}

func (rc *redisCache) DelBean(table string, id string) {
	_ = rc.delObject(rc.getBeanKey(table, id))
}

func (rc *redisCache) ClearIds(table string) {
	_ = rc.delObjects(fmt.Sprintf("%s:%s:*", sqlPrefix, table))
}

func (rc *redisCache) ClearBeans(table string) {
	_ = rc.delObjects(rc.getBeanKey(table, "*"))
}

func (rc *redisCache) serialize(obj interface{}) (data []byte, err error) {
	rc.registerGobConcreteType(obj)

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

func (rc *redisCache) deserialize(data []byte) (ptr interface{}, err error) {
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

func (rc *redisCache) registerGobConcreteType(obj interface{}) {
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
