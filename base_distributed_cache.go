package cache

import (
	`bytes`
	`encoding/gob`
	`fmt`
	`reflect`
	`unsafe`
)

type baseDistributedCache struct {
	baseCache
}

func (bdc *baseDistributedCache) serialize(obj interface{}) (data []byte, err error) {
	bdc.registerGobConcreteType(obj)

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

func (bdc *baseDistributedCache) deserialize(data []byte) (ptr interface{}, err error) {
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

func (bdc *baseDistributedCache) registerGobConcreteType(obj interface{}) {
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
