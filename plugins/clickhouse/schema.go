package clickhouse

import (
	"fmt"

	"github.com/silves-xiang/data-bridge/internal/schema"
	"github.com/silves-xiang/data-bridge/pkg/source"
)

// clickhouseToCommon maps ClickHouse type names to CommonType.
var clickhouseToCommon = map[string]schema.CommonType{
	"Int8":      schema.TypeInt8,
	"Int16":     schema.TypeInt16,
	"Int32":     schema.TypeInt32,
	"Int64":     schema.TypeInt64,
	"UInt8":     schema.TypeUint8,
	"UInt16":    schema.TypeUint16,
	"UInt32":    schema.TypeUint32,
	"UInt64":    schema.TypeUint64,
	"Float32":   schema.TypeFloat32,
	"Float64":   schema.TypeFloat64,
	"Decimal":   schema.TypeDecimal,
	"String":    schema.TypeText,
	"FixedString": schema.TypeBytes,
	"Date":      schema.TypeDate,
	"Date32":    schema.TypeDate,
	"DateTime":  schema.TypeTimestamp,
	"DateTime64": schema.TypeTimestamp,
	"Bool":      schema.TypeBool,
	"UUID":      schema.TypeUUID,
	"Enum8":     schema.TypeEnum,
	"Enum16":    schema.TypeEnum,
	"Array":     schema.TypeArray,
	"JSON":      schema.TypeJSON,
}

// MapSourceType maps a ClickHouse type string to CommonType.
func MapSourceType(nativeType string) (commonType schema.CommonType, length, precision, scale int, err error) {
	// Strip Nullable() wrapper.
	typeName := nativeType
	nullable := false
	if len(typeName) > 9 && typeName[:9] == "Nullable(" {
		typeName = typeName[9 : len(typeName)-1]
		nullable = true
	}
	_ = nullable

	typ, ok := clickhouseToCommon[typeName]
	if !ok {
		// Try base name before '('.
		for i, c := range typeName {
			if c == '(' {
				typ, ok = clickhouseToCommon[typeName[:i]]
				break
			}
		}
	}
	if !ok {
		return schema.TypeInvalid, 0, 0, 0, fmt.Errorf("clickhouse: unknown type %q", nativeType)
	}
	return typ, 0, 0, 0, nil
}

// MapTargetType converts a ColumnInfo to a ClickHouse-native type string.
func MapTargetType(col source.ColumnInfo) string {
	switch schema.CommonType(col.CommonType) {
	case schema.TypeBool:
		return "Bool"
	case schema.TypeInt8:
		return "Int8"
	case schema.TypeInt16:
		return "Int16"
	case schema.TypeInt32:
		return "Int32"
	case schema.TypeInt64:
		return "Int64"
	case schema.TypeUint8:
		return "UInt8"
	case schema.TypeUint16:
		return "UInt16"
	case schema.TypeUint32:
		return "UInt32"
	case schema.TypeUint64:
		return "UInt64"
	case schema.TypeFloat32:
		return "Float32"
	case schema.TypeFloat64:
		return "Float64"
	case schema.TypeDecimal:
		if col.Precision > 0 && col.Scale > 0 {
			return fmt.Sprintf("Decimal(%d,%d)", col.Precision, col.Scale)
		}
		return "Decimal(10,0)"
	case schema.TypeString:
		if col.Length > 0 {
			return fmt.Sprintf("String")
		}
		return "String"
	case schema.TypeText:
		return "String"
	case schema.TypeBytes:
		return "String"
	case schema.TypeBinary:
		return "String"
	case schema.TypeJSON:
		return "String"
	case schema.TypeDate:
		return "Date"
	case schema.TypeTime:
		return "DateTime"
	case schema.TypeTimestamp:
		if col.Precision > 0 {
			return fmt.Sprintf("DateTime64(%d)", col.Precision)
		}
		return "DateTime"
	case schema.TypeTimestampTZ:
		return "DateTime"
	case schema.TypeUUID:
		return "UUID"
	case schema.TypeArray:
		return "String"
	case schema.TypeEnum:
		return "String"
	default:
		return "String"
	}
}
