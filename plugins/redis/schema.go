package redis

import (
	"github.com/silves-xiang/data-bridge/internal/schema"
	"github.com/silves-xiang/data-bridge/pkg/source"
)

// MapSourceType maps a Redis type indicator to CommonType. All Redis hash
// field values are strings.
func MapSourceType(nativeType string) (commonType schema.CommonType, length, precision, scale int, err error) {
	return schema.TypeString, 0, 0, 0, nil
}

// MapTargetType converts a ColumnInfo to a target type string. Redis stores
// hash field values as strings.
func MapTargetType(col source.ColumnInfo) string {
	switch schema.CommonType(col.CommonType) {
	default:
		return "string"
	}
}
