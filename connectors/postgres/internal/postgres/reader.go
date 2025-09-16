package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/data-power-io/noesis-connectors/sdks/go/server"
	noesisv1 "github.com/data-power-io/noesis-protocol/languages/go/datapower/noesis/v1"
	"github.com/jackc/pgx/v5"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	defaultBatchSize = 10000
)

type Reader struct {
	client *Client
	logger *zap.Logger
}

func NewReader(client *Client, logger *zap.Logger) (*Reader, error) {
	return &Reader{
		client: client,
		logger: logger,
	}, nil
}

func (r *Reader) Read(ctx context.Context, req *noesisv1.ReadRequest, stream server.ReadStream) error {
	// Determine which mode is being used
	if fullTable := req.GetFullTable(); fullTable != nil {
		return r.readFullTable(ctx, req, fullTable, stream)
	} else if changeStream := req.GetChangeStream(); changeStream != nil {
		return r.readChangeStream(ctx, req, changeStream, stream)
	} else {
		return status.Errorf(codes.InvalidArgument, "no valid read mode specified")
	}
}

func (r *Reader) readFullTable(ctx context.Context, req *noesisv1.ReadRequest, fullTable *noesisv1.FullTableScan, stream server.ReadStream) error {
	entity := fullTable.Entity
	schema := "public" // Default schema

	columns, err := r.client.GetColumns(ctx, schema, entity)
	if err != nil {
		return status.Errorf(codes.NotFound, "failed to get columns for table %s.%s: %v", schema, entity, err)
	}

	var cursor string
	if fullTable.ResumeFrom != nil {
		cursor = string(fullTable.ResumeFrom.Token)
	}

	query := r.buildSelectQuery(schema, entity, columns, cursor)

	r.logger.Info("Executing full table read",
		zap.String("schema", schema),
		zap.String("entity", entity),
		zap.String("query", query))

	rows, err := r.client.Query(ctx, query)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to execute query: %v", err)
	}
	defer rows.Close()

	return r.streamRows(ctx, rows, columns, stream, req.SessionId)
}

func (r *Reader) readChangeStream(ctx context.Context, req *noesisv1.ReadRequest, changeStream *noesisv1.ChangeStream, stream server.ReadStream) error {
	entity := changeStream.Entity
	schema := "public" // Default schema

	columns, err := r.client.GetColumns(ctx, schema, entity)
	if err != nil {
		return status.Errorf(codes.NotFound, "failed to get columns for table %s.%s: %v", schema, entity, err)
	}

	timestampCol := r.findTimestampColumn(columns)
	if timestampCol == "" {
		return status.Errorf(codes.FailedPrecondition,
			"table %s.%s does not have a suitable timestamp column for change stream reads",
			schema, entity)
	}

	var cursor string
	if changeStream.From != nil {
		cursor = string(changeStream.From.Token)
	}

	query := r.buildIncrementalQuery(schema, entity, columns, timestampCol, cursor)

	r.logger.Info("Executing change stream read",
		zap.String("schema", schema),
		zap.String("entity", entity),
		zap.String("timestamp_column", timestampCol),
		zap.String("cursor", cursor))

	rows, err := r.client.Query(ctx, query)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to execute query: %v", err)
	}
	defer rows.Close()

	return r.streamRows(ctx, rows, columns, stream, req.SessionId)
}

func (r *Reader) buildSelectQuery(schema, entity string, columns []ColumnInfo, cursor string) string {
	var columnNames []string
	for _, col := range columns {
		columnNames = append(columnNames, fmt.Sprintf(`"%s"`, col.Name))
	}

	query := fmt.Sprintf(`SELECT %s FROM "%s"."%s"`,
		joinStrings(columnNames, ", "), schema, entity)

	if cursor != "" {
		if offset, err := strconv.Atoi(cursor); err == nil && offset > 0 {
			query += fmt.Sprintf(" OFFSET %d", offset)
		}
	}

	query += fmt.Sprintf(" LIMIT %d", defaultBatchSize)
	return query
}

func (r *Reader) buildIncrementalQuery(schema, entity string, columns []ColumnInfo, timestampCol, cursor string) string {
	var columnNames []string
	for _, col := range columns {
		columnNames = append(columnNames, fmt.Sprintf(`"%s"`, col.Name))
	}

	query := fmt.Sprintf(`SELECT %s FROM "%s"."%s"`,
		joinStrings(columnNames, ", "), schema, entity)

	if cursor != "" {
		query += fmt.Sprintf(` WHERE "%s" > '%s'`, timestampCol, cursor)
	}

	query += fmt.Sprintf(` ORDER BY "%s" ASC LIMIT %d`, timestampCol, defaultBatchSize)
	return query
}

func (r *Reader) streamRows(ctx context.Context, rows pgx.Rows, columns []ColumnInfo, stream server.ReadStream, sessionID string) error {
	recordCount := 0

	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return status.Errorf(codes.Internal, "failed to scan row: %v", err)
		}

		record, err := r.convertRowToRecord(values, columns)
		if err != nil {
			return status.Errorf(codes.Internal, "failed to convert row: %v", err)
		}

		msg := &noesisv1.ReadMessage{
			Msg: &noesisv1.ReadMessage_Record{
				Record: record,
			},
		}

		if err := stream.Send(msg); err != nil {
			return status.Errorf(codes.Internal, "failed to send record: %v", err)
		}

		recordCount++

		if recordCount%1000 == 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
		}
	}

	if err := rows.Err(); err != nil {
		return status.Errorf(codes.Internal, "error iterating rows: %v", err)
	}

	// Send state message with cursor
	stateMsg := &noesisv1.ReadMessage{
		Msg: &noesisv1.ReadMessage_State{
			State: &noesisv1.StateMsg{
				Cursor: &noesisv1.Cursor{
					Token: []byte(r.generateCursor(recordCount)),
				},
			},
		},
	}

	if err := stream.Send(stateMsg); err != nil {
		return status.Errorf(codes.Internal, "failed to send state: %v", err)
	}

	r.logger.Info("Completed streaming",
		zap.Int("records", recordCount))

	return nil
}

func (r *Reader) convertRowToRecord(values []interface{}, columns []ColumnInfo) (*noesisv1.RecordMsg, error) {
	fields := make(map[string]interface{})

	for i, value := range values {
		if i >= len(columns) {
			continue
		}

		col := columns[i]

		if value == nil {
			fields[col.Name] = nil
		} else {
			// Convert to appropriate type based on PostgreSQL data type
			switch col.DataType {
			case "integer", "int4":
				if v, ok := value.(int32); ok {
					fields[col.Name] = v
				} else if v, ok := value.(int64); ok {
					fields[col.Name] = int32(v)
				}
			case "bigint", "int8":
				if v, ok := value.(int64); ok {
					fields[col.Name] = v
				}
			case "smallint", "int2":
				if v, ok := value.(int16); ok {
					fields[col.Name] = int32(v)
				}
			case "real", "float4":
				if v, ok := value.(float32); ok {
					fields[col.Name] = v
				}
			case "double precision", "float8":
				if v, ok := value.(float64); ok {
					fields[col.Name] = v
				}
			case "boolean", "bool":
				if v, ok := value.(bool); ok {
					fields[col.Name] = v
				}
			case "date", "timestamp", "timestamp without time zone", "timestamp with time zone", "timestamptz":
				if v, ok := value.(time.Time); ok {
					fields[col.Name] = v.Format(time.RFC3339)
				}
			default:
				fields[col.Name] = fmt.Sprintf("%v", value)
			}
		}
	}

	// Convert fields map to JSON payload for now
	payloadBytes, err := json.Marshal(fields)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal fields: %w", err)
	}

	return &noesisv1.RecordMsg{
		Op:      noesisv1.Op_UPSERT, // For full table scans, we treat everything as UPSERT
		Payload: payloadBytes,
	}, nil
}

func (r *Reader) findTimestampColumn(columns []ColumnInfo) string {
	for _, col := range columns {
		if col.DataType == "timestamp" ||
		   col.DataType == "timestamp without time zone" ||
		   col.DataType == "timestamp with time zone" ||
		   col.DataType == "timestamptz" {
			return col.Name
		}
	}

	for _, col := range columns {
		if col.Name == "updated_at" || col.Name == "modified_at" || col.Name == "created_at" {
			return col.Name
		}
	}

	return ""
}

func (r *Reader) generateCursor(recordCount int) string {
	return strconv.Itoa(recordCount)
}

func joinStrings(strings []string, separator string) string {
	if len(strings) == 0 {
		return ""
	}
	if len(strings) == 1 {
		return strings[0]
	}

	result := strings[0]
	for i := 1; i < len(strings); i++ {
		result += separator + strings[i]
	}
	return result
}