package mysql

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/silves-xiang/data-bridge/internal/schema"
	"github.com/silves-xiang/data-bridge/pkg/source"
)

// typePattern matches type definitions like "VARCHAR(255)", "DECIMAL(10,2)", "INT".
var typePattern = regexp.MustCompile(`^(\w+)(?:\((\d+)(?:,(\d+))?\))?`)

// mysqlToCommon maps MySQL type names to CommonType.
// For types with parameters (VARCHAR(255)), the parse function extracts length/precision/scale.
var mysqlToCommon = map[string]struct {
	typ    schema.CommonType
	length int // -1 means from params
	prec   int
	scale  int
}{
	// Integer types
	"TINYINT":  {typ: schema.TypeInt8},
	"SMALLINT": {typ: schema.TypeInt16},
	"MEDIUMINT": {typ: schema.TypeInt32},
	"INT":      {typ: schema.TypeInt32},
	"INTEGER":  {typ: schema.TypeInt32},
	"BIGINT":   {typ: schema.TypeInt64},

	// Unsigned integer types
	"TINYINT UNSIGNED":  {typ: schema.TypeUint8},
	"SMALLINT UNSIGNED": {typ: schema.TypeUint16},
	"MEDIUMINT UNSIGNED": {typ: schema.TypeUint32},
	"INT UNSIGNED":      {typ: schema.TypeUint32},
	"BIGINT UNSIGNED":   {typ: schema.TypeUint64},

	// Float types
	"FLOAT":  {typ: schema.TypeFloat32},
	"DOUBLE": {typ: schema.TypeFloat64},
	"REAL":   {typ: schema.TypeFloat64},

	// Decimal
	"DECIMAL": {typ: schema.TypeDecimal, prec: -1, scale: -1},
	"NUMERIC": {typ: schema.TypeDecimal, prec: -1, scale: -1},

	// String types
	"VARCHAR":  {typ: schema.TypeString, length: -1},
	"CHAR":     {typ: schema.TypeString, length: -1},
	"TINYTEXT": {typ: schema.TypeText},
	"TEXT":     {typ: schema.TypeText},
	"MEDIUMTEXT": {typ: schema.TypeText},
	"LONGTEXT":  {typ: schema.TypeText},

	// Binary types
	"BINARY":    {typ: schema.TypeBytes, length: -1},
	"VARBINARY": {typ: schema.TypeBytes, length: -1},
	"TINYBLOB":  {typ: schema.TypeBinary},
	"BLOB":      {typ: schema.TypeBinary},
	"MEDIUMBLOB": {typ: schema.TypeBinary},
	"LONGBLOB":   {typ: schema.TypeBinary},

	// Date/time types
	"DATE":      {typ: schema.TypeDate},
	"TIME":      {typ: schema.TypeTime},
	"DATETIME":  {typ: schema.TypeTimestamp, prec: -1},
	"TIMESTAMP": {typ: schema.TypeTimestamp, prec: -1},
	"YEAR":      {typ: schema.TypeInt16},

	// Special types
	"JSON": {typ: schema.TypeJSON},
	"ENUM": {typ: schema.TypeEnum},
	"SET":  {typ: schema.TypeEnum},

	// Geometry
	"GEOMETRY": {typ: schema.TypeGeometry},
	"POINT":    {typ: schema.TypeGeometry},
	"LINESTRING": {typ: schema.TypeGeometry},
	"POLYGON":   {typ: schema.TypeGeometry},
}

// MapSourceType parses a MySQL type string and returns the CommonType.
func MapSourceType(nativeType string) (commonType schema.CommonType, length, precision, scale int, err error) {
	upper := strings.ToUpper(strings.TrimSpace(nativeType))

	// Handle unsigned types.
	isUnsigned := strings.Contains(upper, "UNSIGNED")
	baseType := upper
	if isUnsigned {
		baseType = strings.Replace(upper, " UNSIGNED", "", 1)
	}

	matches := typePattern.FindStringSubmatch(baseType)
	if matches == nil {
		return schema.TypeInvalid, 0, 0, 0, fmt.Errorf("mysql: cannot parse type %q", nativeType)
	}

	typeName := matches[1]
	lookup := typeName
	if isUnsigned {
		lookup = typeName + " UNSIGNED"
	}

	info, ok := mysqlToCommon[lookup]
	if !ok {
		// Retry without unsigned modifier.
		info, ok = mysqlToCommon[typeName]
		if !ok {
			return schema.TypeInvalid, 0, 0, 0, fmt.Errorf("mysql: unknown type %q", nativeType)
		}
	}

	l := info.length
	p := info.prec
	s := info.scale

	// Extract length parameter (e.g., VARCHAR(255)).
	if matches[2] != "" {
		if l == -1 {
			l, _ = strconv.Atoi(matches[2])
		}
		if p == -1 {
			p, _ = strconv.Atoi(matches[2])
		}
	}

	// Extract scale parameter (e.g., DECIMAL(10,2)).
	if matches[3] != "" {
		if s == -1 {
			s, _ = strconv.Atoi(matches[3])
		}
	}

	return info.typ, l, p, s, nil
}

// MapTargetType converts a ColumnInfo to a MySQL-native type string.
func MapTargetType(col source.ColumnInfo) string {
	switch schema.CommonType(col.CommonType) {
	case schema.TypeBool:
		return "TINYINT(1)"
	case schema.TypeInt8:
		return "TINYINT"
	case schema.TypeInt16:
		return "SMALLINT"
	case schema.TypeInt32:
		return "INT"
	case schema.TypeInt64:
		return "BIGINT"
	case schema.TypeUint8:
		return "TINYINT UNSIGNED"
	case schema.TypeUint16:
		return "SMALLINT UNSIGNED"
	case schema.TypeUint32:
		return "INT UNSIGNED"
	case schema.TypeUint64:
		return "BIGINT UNSIGNED"
	case schema.TypeFloat32:
		return "FLOAT"
	case schema.TypeFloat64:
		return "DOUBLE"
	case schema.TypeDecimal:
		if col.Precision > 0 && col.Scale > 0 {
			return fmt.Sprintf("DECIMAL(%d,%d)", col.Precision, col.Scale)
		}
		return "DECIMAL(10,0)"
	case schema.TypeString:
		if col.Length > 0 {
			return fmt.Sprintf("VARCHAR(%d)", col.Length)
		}
		return "VARCHAR(255)"
	case schema.TypeText:
		return "TEXT"
	case schema.TypeBytes:
		if col.Length > 0 {
			return fmt.Sprintf("VARBINARY(%d)", col.Length)
		}
		return "BLOB"
	case schema.TypeBinary:
		return "BLOB"
	case schema.TypeJSON:
		return "JSON"
	case schema.TypeDate:
		return "DATE"
	case schema.TypeTime:
		return "TIME"
	case schema.TypeTimestamp:
		if col.Precision > 0 {
			return fmt.Sprintf("DATETIME(%d)", col.Precision)
		}
		return "DATETIME"
	case schema.TypeTimestampTZ:
		if col.Precision > 0 {
			return fmt.Sprintf("TIMESTAMP(%d)", col.Precision)
		}
		return "TIMESTAMP"
	case schema.TypeUUID:
		return "CHAR(36)"
	case schema.TypeEnum:
		return "VARCHAR(255)"
	default:
		return "TEXT"
	}
}
