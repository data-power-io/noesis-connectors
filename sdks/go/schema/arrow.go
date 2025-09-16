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

// SchemaToDescriptor converts an Arrow schema to a SchemaDescriptor proto message
func (m *ArrowSchemaManager) SchemaToDescriptor(schema *arrow.Schema, schemaID string) (*connectorv1.SchemaDescriptor, error) {
	// Serialize schema to Arrow IPC format
	var buf bytes.Buffer
	writer := ipc.NewWriter(&buf, ipc.WithSchema(schema))

	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("failed to serialize Arrow schema: %w", err)
	}

	return &connectorv1.SchemaDescriptor{
		SchemaId: schemaID,
		Spec: &connectorv1.SchemaDescriptor_Arrow{
			Arrow: buf.Bytes(),
		},
	}, nil
}

// DescriptorToSchema converts a SchemaDescriptor proto message to an Arrow schema
func (m *ArrowSchemaManager) DescriptorToSchema(descriptor *connectorv1.SchemaDescriptor) (*arrow.Schema, error) {
	switch spec := descriptor.Spec.(type) {
	case *connectorv1.SchemaDescriptor_Arrow:
		buf := bytes.NewReader(spec.Arrow)
		reader, err := ipc.NewReader(buf)
		if err != nil {
			return nil, fmt.Errorf("failed to create Arrow reader: %w", err)
		}
		defer reader.Release()

		return reader.Schema(), nil

	case *connectorv1.SchemaDescriptor_Json:
		// Convert JSON schema to Arrow schema
		return m.jsonToArrowSchema(spec.Json)

	default:
		return nil, fmt.Errorf("unsupported schema specification type")
	}
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
