// Package schema defines database-agnostic type classification and mapping.
package schema

// CommonType is a database-agnostic type classification.
// It bridges type systems between different databases.
type CommonType int

const (
	TypeInvalid CommonType = iota
	TypeBool
	TypeInt8
	TypeInt16
	TypeInt32
	TypeInt64
	TypeUint8
	TypeUint16
	TypeUint32
	TypeUint64
	TypeFloat32
	TypeFloat64
	TypeDecimal
	TypeString
	TypeText         // Unbounded text (TEXT, CLOB)
	TypeBytes        // Fixed-length binary
	TypeBinary       // Variable-length binary (BLOB, BYTEA)
	TypeJSON
	TypeDate
	TypeTime
	TypeTimestamp
	TypeTimestampTZ
	TypeUUID
	TypeArray
	TypeEnum
	TypeGeometry
)

// String returns a human-readable name for the common type.
func (t CommonType) String() string {
	switch t {
	case TypeBool:
		return "BOOL"
	case TypeInt8:
		return "INT8"
	case TypeInt16:
		return "INT16"
	case TypeInt32:
		return "INT32"
	case TypeInt64:
		return "INT64"
	case TypeUint8:
		return "UINT8"
	case TypeUint16:
		return "UINT16"
	case TypeUint32:
		return "UINT32"
	case TypeUint64:
		return "UINT64"
	case TypeFloat32:
		return "FLOAT32"
	case TypeFloat64:
		return "FLOAT64"
	case TypeDecimal:
		return "DECIMAL"
	case TypeString:
		return "STRING"
	case TypeText:
		return "TEXT"
	case TypeBytes:
		return "BYTES"
	case TypeBinary:
		return "BINARY"
	case TypeJSON:
		return "JSON"
	case TypeDate:
		return "DATE"
	case TypeTime:
		return "TIME"
	case TypeTimestamp:
		return "TIMESTAMP"
	case TypeTimestampTZ:
		return "TIMESTAMPTZ"
	case TypeUUID:
		return "UUID"
	case TypeArray:
		return "ARRAY"
	case TypeEnum:
		return "ENUM"
	case TypeGeometry:
		return "GEOMETRY"
	default:
		return "INVALID"
	}
}
