package schema

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/ipc"
	connectorv1 "github.com/data-power-io/noesis-protocol/languages/go/datapower/noesis/v1"
)

// ArrowSchemaManager provides utilities for working with Apache Arrow schemas
type ArrowSchemaManager struct{}

// NewArrowSchemaManager creates a new Arrow schema manager
func NewArrowSchemaManager() *ArrowSchemaManager {
	return &ArrowSchemaManager{}
}

// SchemaToStructuredDescriptor converts an Arrow schema to a StructuredSchemaDescriptor proto message
// Note: This creates a simplified descriptor. For full Arrow schema support, use ArrowSchemaToBytes.
func (m *ArrowSchemaManager) SchemaToStructuredDescriptor(schema *arrow.Schema, schemaID string) (*connectorv1.StructuredSchemaDescriptor, error) {
	// Convert Arrow schema to field descriptors
	var fields []*connectorv1.FieldDescriptor
	for i := 0; i < int(schema.NumFields()); i++ {
		field := schema.Field(i)
		fieldDescriptor := &connectorv1.FieldDescriptor{
			Name:            field.Name,
			Type:            m.arrowToFieldType(field.Type),
			Nullable:        field.Nullable,
			OrdinalPosition: int32(i + 1),
		}
		fields = append(fields, fieldDescriptor)
	}

	return &connectorv1.StructuredSchemaDescriptor{
		SchemaId: schemaID,
		Fields:   fields,
	}, nil
}

// StructuredDescriptorToSchema converts a StructuredSchemaDescriptor proto message to an Arrow schema
func (m *ArrowSchemaManager) StructuredDescriptorToSchema(descriptor *connectorv1.StructuredSchemaDescriptor) (*arrow.Schema, error) {
	var fields []arrow.Field
	for _, fieldDesc := range descriptor.Fields {
		dataType := m.fieldTypeToArrow(fieldDesc.Type)
		field := arrow.Field{
			Name:     fieldDesc.Name,
			Type:     dataType,
			Nullable: fieldDesc.Nullable,
		}
		fields = append(fields, field)
	}

	return arrow.NewSchema(fields, nil), nil
}

// jsonToArrowSchema converts a JSON schema string to an Arrow schema
func (m *ArrowSchemaManager) jsonToArrowSchema(jsonSchema string) (*arrow.Schema, error) {
	var schemaMap map[string]interface{}
	if err := json.Unmarshal([]byte(jsonSchema), &schemaMap); err != nil {
		return nil, fmt.Errorf("failed to parse JSON schema: %w", err)
	}

	// This is a simplified implementation - in reality, you'd need more sophisticated
	// JSON schema to Arrow schema conversion
	properties, ok := schemaMap["properties"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("JSON schema must have properties field")
	}

	var fields []arrow.Field
	for fieldName, fieldDef := range properties {
		fieldDefMap, ok := fieldDef.(map[string]interface{})
		if !ok {
			continue
		}

		dataType, err := m.jsonTypeToArrowType(fieldDefMap["type"].(string))
		if err != nil {
			return nil, fmt.Errorf("unsupported field type for %s: %w", fieldName, err)
		}

		field := arrow.Field{
			Name:     fieldName,
			Type:     dataType,
			Nullable: true, // Default to nullable
		}
		fields = append(fields, field)
	}

	return arrow.NewSchema(fields, nil), nil
}

// jsonTypeToArrowType converts JSON schema types to Arrow types
func (m *ArrowSchemaManager) jsonTypeToArrowType(jsonType string) (arrow.DataType, error) {
	switch jsonType {
	case "string":
		return arrow.BinaryTypes.String, nil
	case "integer":
		return arrow.PrimitiveTypes.Int64, nil
	case "number":
		return arrow.PrimitiveTypes.Float64, nil
	case "boolean":
		return arrow.FixedWidthTypes.Boolean, nil
	default:
		return nil, fmt.Errorf("unsupported JSON type: %s", jsonType)
	}
}

// BuildSchemaFromFields creates an Arrow schema from a list of field definitions
func (m *ArrowSchemaManager) BuildSchemaFromFields(fields map[string]string) *arrow.Schema {
	var arrowFields []arrow.Field

	for fieldName, fieldType := range fields {
		dataType := m.stringToArrowType(fieldType)
		field := arrow.Field{
			Name:     fieldName,
			Type:     dataType,
			Nullable: true,
		}
		arrowFields = append(arrowFields, field)
	}

	return arrow.NewSchema(arrowFields, nil)
}

// stringToArrowType converts string type names to Arrow types
func (m *ArrowSchemaManager) stringToArrowType(typeStr string) arrow.DataType {
	switch typeStr {
	case "string", "varchar", "text":
		return arrow.BinaryTypes.String
	case "int", "integer", "int32":
		return arrow.PrimitiveTypes.Int32
	case "bigint", "int64":
		return arrow.PrimitiveTypes.Int64
	case "float", "float32":
		return arrow.PrimitiveTypes.Float32
	case "double", "float64":
		return arrow.PrimitiveTypes.Float64
	case "boolean", "bool":
		return arrow.FixedWidthTypes.Boolean
	case "timestamp":
		return arrow.FixedWidthTypes.Timestamp_ms
	case "date":
		return arrow.FixedWidthTypes.Date32
	case "binary", "bytes":
		return arrow.BinaryTypes.Binary
	default:
		// Default to string for unknown types
		return arrow.BinaryTypes.String
	}
}

// SchemaToJSON converts an Arrow schema to JSON representation
func (m *ArrowSchemaManager) SchemaToJSON(schema *arrow.Schema) (string, error) {
	jsonSchema := map[string]interface{}{
		"type":       "object",
		"properties": make(map[string]interface{}),
	}

	properties := jsonSchema["properties"].(map[string]interface{})

	for i := 0; i < int(schema.NumFields()); i++ {
		field := schema.Field(i)
		fieldType := m.arrowTypeToJSONType(field.Type)

		properties[field.Name] = map[string]interface{}{
			"type":     fieldType,
			"nullable": field.Nullable,
		}
	}

	jsonBytes, err := json.MarshalIndent(jsonSchema, "", "  ")
	if err != nil {
		return "", fmt.Errorf("failed to marshal schema to JSON: %w", err)
	}

	return string(jsonBytes), nil
}

// arrowTypeToJSONType converts Arrow types to JSON schema types
func (m *ArrowSchemaManager) arrowTypeToJSONType(arrowType arrow.DataType) string {
	switch arrowType.ID() {
	case arrow.STRING, arrow.BINARY:
		return "string"
	case arrow.INT8, arrow.INT16, arrow.INT32, arrow.INT64,
		arrow.UINT8, arrow.UINT16, arrow.UINT32, arrow.UINT64:
		return "integer"
	case arrow.FLOAT32, arrow.FLOAT64:
		return "number"
	case arrow.BOOL:
		return "boolean"
	case arrow.TIMESTAMP, arrow.DATE32, arrow.DATE64:
		return "string" // ISO format
	default:
		return "string" // Default fallback
	}
}

// arrowToFieldType converts Arrow types to FieldType enum
func (m *ArrowSchemaManager) arrowToFieldType(arrowType arrow.DataType) connectorv1.FieldType {
	switch arrowType.ID() {
	case arrow.STRING, arrow.BINARY:
		return connectorv1.FieldType_FIELD_TYPE_STRING
	case arrow.INT32:
		return connectorv1.FieldType_FIELD_TYPE_INTEGER
	case arrow.INT64:
		return connectorv1.FieldType_FIELD_TYPE_BIGINT
	case arrow.INT16:
		return connectorv1.FieldType_FIELD_TYPE_SMALLINT
	case arrow.FLOAT32:
		return connectorv1.FieldType_FIELD_TYPE_FLOAT
	case arrow.FLOAT64:
		return connectorv1.FieldType_FIELD_TYPE_DOUBLE
	case arrow.BOOL:
		return connectorv1.FieldType_FIELD_TYPE_BOOLEAN
	case arrow.DATE32:
		return connectorv1.FieldType_FIELD_TYPE_DATE
	case arrow.TIMESTAMP:
		return connectorv1.FieldType_FIELD_TYPE_TIMESTAMP
	default:
		return connectorv1.FieldType_FIELD_TYPE_STRING // Default fallback
	}
}

// fieldTypeToArrow converts FieldType enum to Arrow types
func (m *ArrowSchemaManager) fieldTypeToArrow(fieldType connectorv1.FieldType) arrow.DataType {
	switch fieldType {
	case connectorv1.FieldType_FIELD_TYPE_STRING, connectorv1.FieldType_FIELD_TYPE_TEXT:
		return arrow.BinaryTypes.String
	case connectorv1.FieldType_FIELD_TYPE_INTEGER:
		return arrow.PrimitiveTypes.Int32
	case connectorv1.FieldType_FIELD_TYPE_BIGINT:
		return arrow.PrimitiveTypes.Int64
	case connectorv1.FieldType_FIELD_TYPE_SMALLINT:
		return arrow.PrimitiveTypes.Int16
	case connectorv1.FieldType_FIELD_TYPE_DECIMAL:
		return arrow.PrimitiveTypes.Float64 // Simplified mapping
	case connectorv1.FieldType_FIELD_TYPE_FLOAT:
		return arrow.PrimitiveTypes.Float32
	case connectorv1.FieldType_FIELD_TYPE_DOUBLE:
		return arrow.PrimitiveTypes.Float64
	case connectorv1.FieldType_FIELD_TYPE_BOOLEAN:
		return arrow.FixedWidthTypes.Boolean
	case connectorv1.FieldType_FIELD_TYPE_DATE:
		return arrow.FixedWidthTypes.Date32
	case connectorv1.FieldType_FIELD_TYPE_TIME:
		return arrow.FixedWidthTypes.Time32s
	case connectorv1.FieldType_FIELD_TYPE_TIMESTAMP, connectorv1.FieldType_FIELD_TYPE_TIMESTAMP_WITH_TZ:
		return arrow.FixedWidthTypes.Timestamp_ms
	case connectorv1.FieldType_FIELD_TYPE_BINARY:
		return arrow.BinaryTypes.Binary
	case connectorv1.FieldType_FIELD_TYPE_UUID:
		return arrow.BinaryTypes.String // UUIDs as strings
	default:
		return arrow.BinaryTypes.String // Default fallback
	}
}

// ArrowSchemaToBytes serializes an Arrow schema to bytes for storage in payload
func (m *ArrowSchemaManager) ArrowSchemaToBytes(schema *arrow.Schema) ([]byte, error) {
	var buf bytes.Buffer
	writer := ipc.NewWriter(&buf, ipc.WithSchema(schema))
	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("failed to serialize Arrow schema: %w", err)
	}
	return buf.Bytes(), nil
}

// ArrowSchemaFromBytes deserializes an Arrow schema from bytes
func (m *ArrowSchemaManager) ArrowSchemaFromBytes(data []byte) (*arrow.Schema, error) {
	buf := bytes.NewReader(data)
	reader, err := ipc.NewReader(buf)
	if err != nil {
		return nil, fmt.Errorf("failed to create Arrow reader: %w", err)
	}
	defer reader.Release()
	return reader.Schema(), nil
}