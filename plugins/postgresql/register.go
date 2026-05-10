package postgresql

import (
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/silves-xiang/data-bridge/pkg/sink"
	"github.com/silves-xiang/data-bridge/pkg/source"
)

func init() {
	source.Register("postgresql", func() source.Source {
		return &Source{}
	})
	sink.Register("postgresql", func() sink.Sink {
		return &Sink{}
	})
}
