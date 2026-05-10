package clickhouse

import (
	_ "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/silves-xiang/data-bridge/pkg/sink"
	"github.com/silves-xiang/data-bridge/pkg/source"
)

func init() {
	source.Register("clickhouse", func() source.Source {
		return &Source{}
	})
	sink.Register("clickhouse", func() sink.Sink {
		return &Sink{}
	})
}
