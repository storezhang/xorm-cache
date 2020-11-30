package cache

import (
	`fmt`
	`hash/crc32`
)

type baseCache struct {
	options options
}

func (bc *baseCache) getBeanKey(table string, id string) (key string) {
	if "" != bc.options.Prefix {
		key = fmt.Sprintf("%s:%s:%s:%s", beanPrefix, bc.options.Prefix, table, id)
	} else {
		key = fmt.Sprintf("%s:%s:%s", beanPrefix, table, id)
	}

	return
}

func (bc *baseCache) getTableKey(table string) (key string) {
	if "" != bc.options.Prefix {
		key = fmt.Sprintf("%s:%s:%s", beanPrefix, bc.options.Prefix, table)
	} else {
		key = fmt.Sprintf("%s:%s", beanPrefix, table)
	}

	return
}

func (bc *baseCache) getAllTableKey(table string) (key string) {
	if "" != bc.options.Prefix {
		key = fmt.Sprintf("%s:%s:%s:*", beanPrefix, bc.options.Prefix, table)
	} else {
		key = fmt.Sprintf("%s:%s:*", beanPrefix, table)
	}

	return
}

func (bc *baseCache) getSqlKey(table string, sql string) (key string) {
	if "" != bc.options.Prefix {
		key = fmt.Sprintf("%s:%s:%s:%d", sqlPrefix, bc.options.Prefix, table, crc32.ChecksumIEEE([]byte(sql)))
	} else {
		key = fmt.Sprintf("%s:%s:%d", sqlPrefix, table, crc32.ChecksumIEEE([]byte(sql)))
	}

	return
}

func (bc *baseCache) getAllSqlKey(table string) (key string) {
	if "" != bc.options.Prefix {
		key = fmt.Sprintf("%s:%s:%s:*", sqlPrefix, bc.options.Prefix, table)
	} else {
		key = fmt.Sprintf("%s:%s:*", sqlPrefix, table)
	}

	return
}
