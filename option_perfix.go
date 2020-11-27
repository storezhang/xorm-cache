package cache

type optionPrefix struct {
	prefix string
}

// WithPrefix 配置过期时间
func WithPrefix(prefix string) *optionPrefix {
	return &optionPrefix{prefix: prefix}
}

func (oe *optionPrefix) apply(options *options) {
	options.Prefix = oe.prefix
}
