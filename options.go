package cache

import (
	`time`
)

type options struct {
	// Expiration 过期时间
	Expiration time.Duration
	// Prefix 用来区别不同系统的缓存，原理就是在生成的缓存Key中，增加Prefix来做最终标识
	// 如果原来生成的Key是：bean:client:1，Prefix是1的话，那么生成的最终Key是bean:1:client:1
	Prefix string
}

func defaultOptions() options {
	return options{Expiration: 5 * time.Minute}
}
