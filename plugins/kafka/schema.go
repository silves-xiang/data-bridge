package kafka

import (
	"github.com/silves-xiang/data-bridge/internal/schema"
	"github.com/silves-xiang/data-bridge/pkg/source"
)

// MapSourceType maps a Kafka type to CommonType. All values are strings.
func MapSourceType(nativeType string) (commonType schema.CommonType, length, precision, scale int, err error) {
	return schema.TypeString, 0, 0, 0, nil
}

// MapTargetType converts a ColumnInfo to a target type string.
func MapTargetType(col source.ColumnInfo) string {
	return "string"
}
