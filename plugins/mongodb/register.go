package mongodb

import (
	_ "go.mongodb.org/mongo-driver/mongo"

	"github.com/silves-xiang/data-bridge/pkg/sink"
	"github.com/silves-xiang/data-bridge/pkg/source"
)

func init() {
	source.Register("mongodb", func() source.Source {
		return &Source{}
	})
	sink.Register("mongodb", func() sink.Sink {
		return &Sink{}
	})
}
