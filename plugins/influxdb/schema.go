package influxdb

import (
	"fmt"

	"github.com/silves-xiang/data-bridge/internal/schema"
	"github.com/silves-xiang/data-bridge/pkg/source"
)

// MapSourceType maps an InfluxDB native type string to the CommonType enum.
func MapSourceType(nativeType string) (commonType schema.CommonType, length, precision, scale int, err error) {
	switch nativeType {
	case "string":
		return schema.TypeString, 0, 0, 0, nil
	case "double", "float":
		return schema.TypeFloat64, 0, 0, 0, nil
	case "long", "integer":
		return schema.TypeInt64, 0, 0, 0, nil
	case "unsignedLong":
		return schema.TypeUint64, 0, 0, 0, nil
	case "boolean":
		return schema.TypeBool, 0, 0, 0, nil
	case "dateTime", "dateTime:RFC3339", "dateTime:RFC3339Nano":
		return schema.TypeTimestamp, 0, 0, 0, nil
	default:
		return schema.TypeInvalid, 0, 0, 0, fmt.Errorf("influxdb: unknown type %q", nativeType)
	}
}

// MapTargetType converts a ColumnInfo to an InfluxDB field type string.
func MapTargetType(col source.ColumnInfo) string {
	switch schema.CommonType(col.CommonType) {
	case schema.TypeBool:
		return "BOOLEAN"
	case schema.TypeInt8, schema.TypeInt16, schema.TypeInt32, schema.TypeInt64:
		return "LONG"
	case schema.TypeUint8, schema.TypeUint16, schema.TypeUint32, schema.TypeUint64:
		return "UNSIGNEDLONG"
	case schema.TypeFloat32, schema.TypeFloat64:
		return "DOUBLE"
	case schema.TypeString, schema.TypeText, schema.TypeUUID, schema.TypeEnum:
		return "STRING"
	case schema.TypeDate, schema.TypeTime, schema.TypeTimestamp, schema.TypeTimestampTZ:
		return "DATETIME"
	default:
		return "STRING"
	}
}
