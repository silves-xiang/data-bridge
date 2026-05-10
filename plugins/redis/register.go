package redis

import (
	_ "github.com/redis/go-redis/v9"

	"github.com/silves-xiang/data-bridge/pkg/sink"
	"github.com/silves-xiang/data-bridge/pkg/source"
)

func init() {
	source.Register("redis", func() source.Source {
		return &Source{}
	})
	sink.Register("redis", func() sink.Sink {
		return &Sink{}
	})
}
