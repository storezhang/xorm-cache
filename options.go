package cache

import (
	`time`
)

type options struct {
	// Expiration 过期时间
	Expiration time.Duration
}

func defaultOptions() options {
	return options{Expiration: 5 * time.Minute}
}
