package influxdb

import (
	"github.com/silves-xiang/data-bridge/pkg/sink"
	"github.com/silves-xiang/data-bridge/pkg/source"
)

func init() {
	source.Register("influxdb", func() source.Source {
		return &Source{}
	})
	sink.Register("influxdb", func() sink.Sink {
		return &Sink{}
	})
}
