package kafka

import (
	_ "github.com/IBM/sarama"

	"github.com/silves-xiang/data-bridge/pkg/sink"
	"github.com/silves-xiang/data-bridge/pkg/source"
)

func init() {
	source.Register("kafka", func() source.Source {
		return &Source{}
	})
	sink.Register("kafka", func() sink.Sink {
		return &Sink{}
	})
}
