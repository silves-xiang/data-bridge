package mongodb

import (
	"fmt"

	"github.com/silves-xiang/data-bridge/internal/schema"
	"github.com/silves-xiang/data-bridge/pkg/source"
)

// MapSourceType maps a MongoDB BSON type string to CommonType.
func MapSourceType(nativeType string) (commonType schema.CommonType, length, precision, scale int, err error) {
	switch nativeType {
	case "double":
		return schema.TypeFloat64, 0, 0, 0, nil
	case "string":
		return schema.TypeString, 0, 0, 0, nil
	case "object":
		return schema.TypeJSON, 0, 0, 0, nil
	case "array":
		return schema.TypeArray, 0, 0, 0, nil
	case "binData":
		return schema.TypeBinary, 0, 0, 0, nil
	case "objectId":
		return schema.TypeString, 0, 0, 0, nil
	case "bool":
		return schema.TypeBool, 0, 0, 0, nil
	case "date":
		return schema.TypeTimestamp, 0, 0, 0, nil
	case "null":
		return schema.TypeString, 0, 0, 0, nil
	case "int":
		return schema.TypeInt32, 0, 0, 0, nil
	case "long":
		return schema.TypeInt64, 0, 0, 0, nil
	case "decimal":
		return schema.TypeDecimal, 0, 0, 0, nil
	default:
		return schema.TypeString, 0, 0, 0, nil
	}
}

// MapTargetType converts a ColumnInfo to a MongoDB BSON type string.
func MapTargetType(col source.ColumnInfo) string {
	switch schema.CommonType(col.CommonType) {
	case schema.TypeBool:
		return "bool"
	case schema.TypeInt8, schema.TypeInt16, schema.TypeInt32:
		return "int"
	case schema.TypeInt64:
		return "long"
	case schema.TypeUint8, schema.TypeUint16, schema.TypeUint32, schema.TypeUint64:
		return "long"
	case schema.TypeFloat32, schema.TypeFloat64:
		return "double"
	case schema.TypeDecimal:
		return "decimal"
	case schema.TypeString, schema.TypeText, schema.TypeUUID, schema.TypeEnum:
		return "string"
	case schema.TypeDate, schema.TypeTime, schema.TypeTimestamp, schema.TypeTimestampTZ:
		return "date"
	case schema.TypeJSON, schema.TypeArray:
		return fmt.Sprintf("object")
	case schema.TypeBinary, schema.TypeBytes:
		return "binData"
	default:
		return "string"
	}
}
