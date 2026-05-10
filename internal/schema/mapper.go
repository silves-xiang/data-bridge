package schema

import "github.com/silves-xiang/data-bridge/pkg/source"

// SourceTypeMapper maps a native type string to a CommonType and its parameters.
// Each source plugin implements this to classify its database types.
type SourceTypeMapper interface {
	// MapSourceType parses a native type string (e.g., "DATETIME(3)")
	// and returns the CommonType with normalized precision/scale/length.
	MapSourceType(nativeType string) (commonType CommonType, length, precision, scale int, err error)
}

// TargetTypeMapper maps a CommonType back to a target-native type string.
// Each sink plugin implements this to generate DDL for its database.
type TargetTypeMapper interface {
	// MapTargetType converts a column's CommonType and parameters to a
	// target-native type string suitable for CREATE TABLE statements.
	MapTargetType(col source.ColumnInfo) string
}

// SchemaMapper combines both directions of type mapping.
// Plugins implement this to support both source and sink roles.
type SchemaMapper interface {
	SourceTypeMapper
	TargetTypeMapper
}
