package mysql

import (
	_ "github.com/go-sql-driver/mysql"

	"github.com/silves-xiang/data-bridge/pkg/sink"
	"github.com/silves-xiang/data-bridge/pkg/source"
)

func init() {
	source.Register("mysql", func() source.Source {
		return &Source{}
	})
	sink.Register("mysql", func() sink.Sink {
		return &Sink{}
	})
}
