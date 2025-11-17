package streaming

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/arrow/memory"
	connectorv1 "github.com/data-power-io/noesis-protocol/languages/go/datapower/noesis/v1"
	"go.uber.org/zap"
)

// RecordStreamer provides utilities for streaming records efficiently
type RecordStreamer struct {
	stream    RecordStream
	logger    *zap.Logger
	schema    *arrow.Schema
	schemaID  string
	batchSize int
	buffer    []arrow.Record
	mutex     sync.Mutex

	// Metrics
	recordsEmitted int64
	bytesEmitted   int64
	lastEmitTime   time.Time
}

// RecordStream defines the interface for sending streaming messages
type RecordStream interface {
	Send(msg *connectorv1.ReadMessage) error
	Context() context.Context
}

// NewRecordStreamer creates a new record streamer
func NewRecordStreamer(stream RecordStream, logger *zap.Logger, schema *arrow.Schema, schemaID string) *RecordStreamer {
	return &RecordStreamer{
		stream:       stream,
		logger:       logger,
		schema:       schema,
		schemaID:     schemaID,
		batchSize:    1000, // Default batch size
		buffer:       make([]arrow.Record, 0),
		lastEmitTime: time.Now(),
	}
}

// SetBatchSize sets the batch size for record streaming
func (rs *RecordStreamer) SetBatchSize(size int) {
	rs.batchSize = size
}

// SendSchema sends a schema message to the stream using StructuredSchemaDescriptor
func (rs *RecordStreamer) SendSchema(entity string, schema *connectorv1.StructuredSchemaDescriptor) error {
	msg := &connectorv1.ReadMessage{
		Msg: &connectorv1.ReadMessage_Schema{
			Schema: &connectorv1.SchemaMsg{
				Entity: entity,
				Schema: schema,
			},
		},
	}

	return rs.stream.Send(msg)
}


// SendState sends a state checkpoint message
func (rs *RecordStreamer) SendState(cursor *connectorv1.Cursor, watermark int64, groupID string) error {
	msg := &connectorv1.ReadMessage{
		Msg: &connectorv1.ReadMessage_State{
			State: &connectorv1.StateMsg{
				Cursor:    cursor,
				Watermark: watermark,
				GroupId:   groupID,
			},
		},
	}

	return rs.stream.Send(msg)
}

// SendLog sends a log message
func (rs *RecordStreamer) SendLog(level, message string, kv map[string]string) error {
	msg := &connectorv1.ReadMessage{
		Msg: &connectorv1.ReadMessage_Log{
			Log: &connectorv1.LogMsg{
				Level:   level,
				Message: message,
				Kv:      kv,
			},
		},
	}

	return rs.stream.Send(msg)
}

// SendMetric sends a metric message
func (rs *RecordStreamer) SendMetric(name string, value float64, tags map[string]string) error {
	msg := &connectorv1.ReadMessage{
		Msg: &connectorv1.ReadMessage_Metric{
			Metric: &connectorv1.MetricMsg{
				Name:  name,
				Value: value,
				Tags:  tags,
			},
		},
	}

	return rs.stream.Send(msg)
}

// SendBatchMetrics sends current streaming metrics
func (rs *RecordStreamer) SendBatchMetrics() error {
	tags := map[string]string{
		"schema_id": rs.schemaID,
	}

	// Send records emitted metric
	if err := rs.SendMetric("records_emitted", float64(rs.recordsEmitted), tags); err != nil {
		return err
	}

	// Send bytes emitted metric
	if err := rs.SendMetric("bytes_emitted", float64(rs.bytesEmitted), tags); err != nil {
		return err
	}

	// Send throughput metric
	if !rs.lastEmitTime.IsZero() {
		duration := time.Since(rs.lastEmitTime)
		if duration > 0 {
			throughput := float64(rs.recordsEmitted) / duration.Seconds()
			if err := rs.SendMetric("records_per_second", throughput, tags); err != nil {
				return err
			}
		}
	}

	return nil
}

// extractScalarValue extracts a scalar value from an Arrow array at a given index
func (rs *RecordStreamer) extractScalarValue(arr arrow.Array, index int) interface{} {
	if arr.IsNull(index) {
		return nil
	}

	switch a := arr.(type) {
	case *array.String:
		return a.Value(index)
	case *array.Int32:
		return a.Value(index)
	case *array.Int64:
		return a.Value(index)
	case *array.Float32:
		return a.Value(index)
	case *array.Float64:
		return a.Value(index)
	case *array.Boolean:
		return a.Value(index)
	case *array.Timestamp:
		return a.Value(index).ToTime(arrow.TimeUnit(a.DataType().(*arrow.TimestampType).Unit))
	case *array.Date32:
		return a.Value(index).ToTime()
	case *array.Binary:
		return a.Value(index)
	default:
		// Fallback to string representation
		return fmt.Sprintf("%v", arr.GetOneForMarshal(index))
	}
}

// serializeArrowSchema serializes the Arrow schema to IPC format
func (rs *RecordStreamer) serializeArrowSchema() ([]byte, error) {
	// This would use Arrow IPC writer to serialize the schema
	// For now, return a placeholder - in real implementation, use:
	// var buf bytes.Buffer
	// writer := ipc.NewWriter(&buf, ipc.WithSchema(rs.schema))
	// writer.Close()
	// return buf.Bytes(), nil

	// Placeholder implementation
	schemaJSON := make(map[string]interface{})
	schemaJSON["fields"] = make([]map[string]interface{}, 0)

	for i := 0; i < int(rs.schema.NumFields()); i++ {
		field := rs.schema.Field(i)
		fieldInfo := map[string]interface{}{
			"name":     field.Name,
			"type":     field.Type.String(),
			"nullable": field.Nullable,
		}
		schemaJSON["fields"] = append(schemaJSON["fields"].([]map[string]interface{}), fieldInfo)
	}

	return json.Marshal(schemaJSON)
}

// RecordBuilder helps build Arrow records for streaming
type RecordBuilder struct {
	schema  *arrow.Schema
	pool    memory.Allocator
	builder *array.RecordBuilder
}

// NewRecordBuilder creates a new record builder
func NewRecordBuilder(schema *arrow.Schema, pool memory.Allocator) *RecordBuilder {
	builder := array.NewRecordBuilder(pool, schema)
	return &RecordBuilder{
		schema:  schema,
		pool:    pool,
		builder: builder,
	}
}

// AddRow adds a row of data to the record builder
func (rb *RecordBuilder) AddRow(values map[string]interface{}) error {
	for i := 0; i < int(rb.schema.NumFields()); i++ {
		field := rb.schema.Field(i)
		value, exists := values[field.Name]

		if !exists || value == nil {
			rb.builder.Field(i).AppendNull()
			continue
		}

		err := rb.appendValue(rb.builder.Field(i), field.Type, value)
		if err != nil {
			return fmt.Errorf("failed to append value for field %s: %w", field.Name, err)
		}
	}

	return nil
}

// Build creates an Arrow record from the accumulated data
func (rb *RecordBuilder) Build() arrow.Record {
	record := rb.builder.NewRecord()
	return record
}

// Reset clears the builder for reuse
func (rb *RecordBuilder) Reset() {
	for i := 0; i < len(rb.schema.Fields()); i++ {
		rb.builder.Field(i).Release()
	}
}

// appendValue appends a value to an array builder based on the Arrow type
func (rb *RecordBuilder) appendValue(builder array.Builder, dataType arrow.DataType, value interface{}) error {
	switch dataType.ID() {
	case arrow.STRING:
		if strBuilder, ok := builder.(*array.StringBuilder); ok {
			if str, ok := value.(string); ok {
				strBuilder.Append(str)
			} else {
				strBuilder.Append(fmt.Sprintf("%v", value))
			}
		}
	case arrow.INT32:
		if intBuilder, ok := builder.(*array.Int32Builder); ok {
			switch v := value.(type) {
			case int:
				intBuilder.Append(int32(v))
			case int32:
				intBuilder.Append(v)
			case int64:
				intBuilder.Append(int32(v))
			case float64:
				intBuilder.Append(int32(v))
			}
		}
	case arrow.INT64:
		if intBuilder, ok := builder.(*array.Int64Builder); ok {
			switch v := value.(type) {
			case int:
				intBuilder.Append(int64(v))
			case int32:
				intBuilder.Append(int64(v))
			case int64:
				intBuilder.Append(v)
			case float64:
				intBuilder.Append(int64(v))
			}
		}
	case arrow.FLOAT64:
		if floatBuilder, ok := builder.(*array.Float64Builder); ok {
			switch v := value.(type) {
			case float32:
				floatBuilder.Append(float64(v))
			case float64:
				floatBuilder.Append(v)
			case int:
				floatBuilder.Append(float64(v))
			case int64:
				floatBuilder.Append(float64(v))
			}
		}
	case arrow.BOOL:
		if boolBuilder, ok := builder.(*array.BooleanBuilder); ok {
			if b, ok := value.(bool); ok {
				boolBuilder.Append(b)
			}
		}
	default:
		// Fallback to string representation
		if strBuilder, ok := builder.(*array.StringBuilder); ok {
			strBuilder.Append(fmt.Sprintf("%v", value))
		}
	}

	return nil
}
