package postgresql

import (
	"fmt"

	"github.com/silves-xiang/data-bridge/internal/schema"
	"github.com/silves-xiang/data-bridge/pkg/source"
)

// MapSourceType parses a PostgreSQL type string and returns the CommonType.
func MapSourceType(nativeType string) (commonType schema.CommonType, length, precision, scale int, err error) {
	// Extract type name and parameters.
	typeName := nativeType
	// Use simple heuristic: find '(' and extract params.
	if idx := findParen(nativeType); idx >= 0 {
		typeName = nativeType[:idx]
		// Extract parameters.
		params := nativeType[idx+1 : len(nativeType)-1]
		if commaIdx := findComma(params); commaIdx >= 0 {
			fmt.Sscanf(params, "%d,%d", &precision, &scale)
		} else {
			fmt.Sscanf(params, "%d", &length)
		}
	}

	pgToCommon := map[string]schema.CommonType{
		// Integer types
		"SMALLINT":   schema.TypeInt16,
		"INTEGER":    schema.TypeInt32,
		"INT":        schema.TypeInt32,
		"INT2":       schema.TypeInt16,
		"INT4":       schema.TypeInt32,
		"INT8":       schema.TypeInt64,
		"BIGINT":     schema.TypeInt64,
		"BIGSERIAL":  schema.TypeInt64,
		"SERIAL":     schema.TypeInt32,
		"SMALLSERIAL": schema.TypeInt16,

		// Float types
		"REAL":             schema.TypeFloat32,
		"FLOAT4":           schema.TypeFloat32,
		"DOUBLE PRECISION": schema.TypeFloat64,
		"FLOAT8":           schema.TypeFloat64,

		// Decimal
		"DECIMAL": schema.TypeDecimal,
		"NUMERIC": schema.TypeDecimal,

		// String types
		"VARCHAR":       schema.TypeString,
		"CHARACTER VARYING": schema.TypeString,
		"CHAR":      schema.TypeString,
		"CHARACTER": schema.TypeString,
		"TEXT":      schema.TypeText,
		"BPCHAR":    schema.TypeString,

		// Binary types
		"BYTEA": schema.TypeBinary,

		// Date/time
		"DATE":           schema.TypeDate,
		"TIME":           schema.TypeTime,
		"TIMETZ":         schema.TypeTime,
		"TIMESTAMP":      schema.TypeTimestamp,
		"TIMESTAMPTZ":    schema.TypeTimestampTZ,
		"TIMESTAMP WITH TIME ZONE":    schema.TypeTimestampTZ,
		"TIMESTAMP WITHOUT TIME ZONE": schema.TypeTimestamp,
		"INTERVAL":       schema.TypeString,

		// Special types
		"JSON":     schema.TypeJSON,
		"JSONB":    schema.TypeJSON,
		"UUID":     schema.TypeUUID,
		"BOOLEAN":  schema.TypeBool,
		"BOOL":     schema.TypeBool,

		// Array
		"TEXT[]":   schema.TypeArray,
		"INTEGER[]": schema.TypeArray,
		"VARCHAR[]": schema.TypeArray,

		// Geometry
		"GEOMETRY": schema.TypeGeometry,
		"GEOGRAPHY": schema.TypeGeometry,
		"POINT":    schema.TypeGeometry,
		"LINESTRING": schema.TypeGeometry,
		"POLYGON":  schema.TypeGeometry,
	}

	upperType := nativeType // Keep original case for display.
	typ, ok := pgToCommon[typeName]
	if !ok {
		return schema.TypeInvalid, 0, 0, 0, fmt.Errorf("postgresql: unknown type %q", upperType)
	}

	return typ, length, precision, scale, nil
}

// MapTargetType converts a ColumnInfo to a PostgreSQL-native type string.
func MapTargetType(col source.ColumnInfo) string {
	switch schema.CommonType(col.CommonType) {
	case schema.TypeBool:
		return "BOOLEAN"
	case schema.TypeInt8:
		return "SMALLINT"
	case schema.TypeInt16:
		return "SMALLINT"
	case schema.TypeInt32:
		return "INTEGER"
	case schema.TypeInt64:
		return "BIGINT"
	case schema.TypeUint8:
		return "SMALLINT"
	case schema.TypeUint16:
		return "INTEGER"
	case schema.TypeUint32:
		return "BIGINT"
	case schema.TypeUint64:
		return "NUMERIC(20)"
	case schema.TypeFloat32:
		return "REAL"
	case schema.TypeFloat64:
		return "DOUBLE PRECISION"
	case schema.TypeDecimal:
		if col.Precision > 0 && col.Scale > 0 {
			return fmt.Sprintf("NUMERIC(%d,%d)", col.Precision, col.Scale)
		}
		return "NUMERIC(10,0)"
	case schema.TypeString:
		if col.Length > 0 {
			return fmt.Sprintf("VARCHAR(%d)", col.Length)
		}
		return "VARCHAR(255)"
	case schema.TypeText:
		return "TEXT"
	case schema.TypeBytes:
		return "BYTEA"
	case schema.TypeBinary:
		return "BYTEA"
	case schema.TypeJSON:
		return "JSONB"
	case schema.TypeDate:
		return "DATE"
	case schema.TypeTime:
		return "TIME"
	case schema.TypeTimestamp:
		if col.Precision > 0 {
			return fmt.Sprintf("TIMESTAMP(%d)", col.Precision)
		}
		return "TIMESTAMP"
	case schema.TypeTimestampTZ:
		if col.Precision > 0 {
			return fmt.Sprintf("TIMESTAMPTZ(%d)", col.Precision)
		}
		return "TIMESTAMPTZ"
	case schema.TypeUUID:
		return "UUID"
	case schema.TypeArray:
		return "TEXT[]"
	case schema.TypeEnum:
		return "VARCHAR(255)"
	default:
		return "TEXT"
	}
}

func findParen(s string) int {
	for i, c := range s {
		if c == '(' {
			return i
		}
	}
	return -1
}

func findComma(s string) int {
	for i, c := range s {
		if c == ',' {
			return i
		}
	}
	return -1
}
