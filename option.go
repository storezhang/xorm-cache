package cache

type option interface {
	apply(options *options)
}
