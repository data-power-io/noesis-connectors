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
	"github.com/jackc/pgx/v5/pgtype"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	defaultBatchSize = 10000
)

type Reader struct {
	client *Client
	config map[string]string
	logger *zap.Logger
}

func NewReader(client *Client, config map[string]string, logger *zap.Logger) (*Reader, error) {
	return &Reader{
		client: client,
		config: config,
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
	schema := r.config["schema"]

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

	return r.streamRows(ctx, rows, columns, stream, req.SessionId, entity)
}

func (r *Reader) readChangeStream(ctx context.Context, req *noesisv1.ReadRequest, changeStream *noesisv1.ChangeStream, stream server.ReadStream) error {
	entity := changeStream.Entity
	schema := r.config["schema"]

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

	return r.streamRows(ctx, rows, columns, stream, req.SessionId, entity)
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

func (r *Reader) streamRows(ctx context.Context, rows pgx.Rows, columns []ColumnInfo, stream server.ReadStream, sessionID string, entity string) error {
	recordCount := 0

	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return status.Errorf(codes.Internal, "failed to scan row: %v", err)
		}

		record, err := r.convertRowToRecord(values, columns, entity)
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

func (r *Reader) convertRowToRecord(values []interface{}, columns []ColumnInfo, entity string) (*noesisv1.RecordMsg, error) {
	rowColumns := make(map[string]*noesisv1.Value)

	for i, value := range values {
		if i >= len(columns) {
			continue
		}

		col := columns[i]

		if value == nil {
			rowColumns[col.Name] = &noesisv1.Value{
				Kind: &noesisv1.Value_NullVal{NullVal: noesisv1.NullValue_NULL_VALUE},
			}
		} else {
			// Convert to appropriate typed Value based on PostgreSQL data type
			switch col.DataType {
			case "integer", "int4":
				if v, ok := value.(int32); ok {
					rowColumns[col.Name] = &noesisv1.Value{
						Kind: &noesisv1.Value_Int32Val{Int32Val: v},
					}
				} else if v, ok := value.(int64); ok {
					rowColumns[col.Name] = &noesisv1.Value{
						Kind: &noesisv1.Value_Int32Val{Int32Val: int32(v)},
					}
				} else if v, ok := value.(int); ok {
					rowColumns[col.Name] = &noesisv1.Value{
						Kind: &noesisv1.Value_Int32Val{Int32Val: int32(v)},
					}
				} else {
					// Fallback: try to parse as string if numeric conversion fails
					r.logger.Warn("Unexpected type for integer column, using default handler",
						zap.String("column", col.Name),
						zap.String("type", fmt.Sprintf("%T", value)))
					rowColumns[col.Name] = &noesisv1.Value{
						Kind: &noesisv1.Value_StringVal{StringVal: fmt.Sprintf("%v", value)},
					}
				}
			case "bigint", "int8":
				if v, ok := value.(int64); ok {
					rowColumns[col.Name] = &noesisv1.Value{
						Kind: &noesisv1.Value_Int64Val{Int64Val: v},
					}
				} else if v, ok := value.(int); ok {
					rowColumns[col.Name] = &noesisv1.Value{
						Kind: &noesisv1.Value_Int64Val{Int64Val: int64(v)},
					}
				} else {
					r.logger.Warn("Unexpected type for bigint column, using default handler",
						zap.String("column", col.Name),
						zap.String("type", fmt.Sprintf("%T", value)))
					rowColumns[col.Name] = &noesisv1.Value{
						Kind: &noesisv1.Value_StringVal{StringVal: fmt.Sprintf("%v", value)},
					}
				}
			case "smallint", "int2":
				if v, ok := value.(int16); ok {
					rowColumns[col.Name] = &noesisv1.Value{
						Kind: &noesisv1.Value_Int32Val{Int32Val: int32(v)},
					}
				} else if v, ok := value.(int); ok {
					rowColumns[col.Name] = &noesisv1.Value{
						Kind: &noesisv1.Value_Int32Val{Int32Val: int32(v)},
					}
				} else {
					r.logger.Warn("Unexpected type for smallint column, using default handler",
						zap.String("column", col.Name),
						zap.String("type", fmt.Sprintf("%T", value)))
					rowColumns[col.Name] = &noesisv1.Value{
						Kind: &noesisv1.Value_StringVal{StringVal: fmt.Sprintf("%v", value)},
					}
				}
			case "real", "float4":
				if v, ok := value.(float32); ok {
					rowColumns[col.Name] = &noesisv1.Value{
						Kind: &noesisv1.Value_FloatVal{FloatVal: v},
					}
				}
			case "double precision", "float8":
				if v, ok := value.(float64); ok {
					rowColumns[col.Name] = &noesisv1.Value{
						Kind: &noesisv1.Value_DoubleVal{DoubleVal: v},
					}
				}
			case "boolean", "bool":
				if v, ok := value.(bool); ok {
					rowColumns[col.Name] = &noesisv1.Value{
						Kind: &noesisv1.Value_BoolVal{BoolVal: v},
					}
				}
			case "date", "timestamp", "timestamp without time zone", "timestamp with time zone", "timestamptz":
				if v, ok := value.(time.Time); ok {
					// Convert to microseconds since epoch
					rowColumns[col.Name] = &noesisv1.Value{
						Kind: &noesisv1.Value_TimestampMicros{TimestampMicros: v.UnixMicro()},
					}
				}
			case "numeric", "decimal":
				// Handle PostgreSQL numeric/decimal types
				var numStr string
				switch v := value.(type) {
				case pgtype.Numeric:
					// Convert pgtype.Numeric to string using Float64Value
					if v.Valid {
						if f64, err := v.Float64Value(); err == nil {
							numStr = strconv.FormatFloat(f64.Float64, 'f', -1, 64)
						} else {
							// Fallback
							numStr = "0"
						}
					} else {
						numStr = ""
					}
				case string:
					numStr = v
				case float64:
					numStr = strconv.FormatFloat(v, 'f', -1, 64)
				case float32:
					numStr = strconv.FormatFloat(float64(v), 'f', -1, 32)
				default:
					// Last resort fallback
					numStr = fmt.Sprintf("%v", value)
				}
				rowColumns[col.Name] = &noesisv1.Value{
					Kind: &noesisv1.Value_StringVal{StringVal: numStr},
				}
			default:
				// For any other type, convert to string
				rowColumns[col.Name] = &noesisv1.Value{
					Kind: &noesisv1.Value_StringVal{StringVal: fmt.Sprintf("%v", value)},
				}
			}
		}
	}

	return &noesisv1.RecordMsg{
		Entity: entity,
		Op:     noesisv1.Op_UPSERT, // For full table scans, we treat everything as UPSERT
		Data: &noesisv1.Row{
			Columns: rowColumns,
		},
	}, nil
}

func (r *Reader) ReadSplit(ctx context.Context, req *noesisv1.ReadSplitRequest, stream server.ReadStream) error {
	entity := req.Entity
	schema := r.config["schema"]

	// Decode split token
	var splitToken SplitToken
	if err := json.Unmarshal(req.Split.SplitToken, &splitToken); err != nil {
		return status.Errorf(codes.InvalidArgument, "failed to decode split token: %v", err)
	}

	r.logger.Info("Reading split",
		zap.String("entity", entity),
		zap.String("split_id", req.Split.SplitId),
		zap.String("strategy", string(splitToken.Strategy)),
		zap.Int("split_index", splitToken.SplitIndex))

	// Get columns for the table
	columns, err := r.client.GetColumns(ctx, schema, entity)
	if err != nil {
		return status.Errorf(codes.NotFound, "failed to get columns for table %s.%s: %v", schema, entity, err)
	}

	// Apply projection if specified
	if req.Projection != nil && len(req.Projection.Columns) > 0 {
		columns = r.filterColumns(columns, req.Projection.Columns)
	}

	// Build query based on split strategy
	query := r.buildSplitQuery(schema, entity, columns, &splitToken)

	r.logger.Debug("Executing split query", zap.String("query", query))

	rows, err := r.client.Query(ctx, query)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to execute split query: %v", err)
	}
	defer rows.Close()

	return r.streamRows(ctx, rows, columns, stream, req.Split.SplitId, entity)
}

func (r *Reader) buildSplitQuery(schema, entity string, columns []ColumnInfo, token *SplitToken) string {
	var columnNames []string
	for _, col := range columns {
		columnNames = append(columnNames, fmt.Sprintf(`"%s"`, col.Name))
	}

	query := fmt.Sprintf(`SELECT %s FROM "%s"."%s"`,
		joinStrings(columnNames, ", "), schema, entity)

	// Build WHERE clause based on split strategy
	var whereClauses []string

	switch token.Strategy {
	case SplitStrategyPrimaryKey:
		// WHERE column >= min AND column < max
		if token.Column != "" && token.MinValue != nil && token.MaxValue != nil {
			whereClauses = append(whereClauses,
				fmt.Sprintf(`"%s" >= %v AND "%s" < %v`, token.Column, token.MinValue, token.Column, token.MaxValue))
		}

	case SplitStrategyRowNumber:
		// Will use OFFSET and LIMIT at the end
		// No WHERE clause needed

	case SplitStrategyModulo:
		// WHERE hashtext(column::text) % modulo = split_index
		if token.Column != "" && token.Modulo > 0 {
			whereClauses = append(whereClauses,
				fmt.Sprintf(`hashtext("%s"::text) %% %d = %d`, token.Column, token.Modulo, token.SplitIndex))
		}
	}

	// Apply WHERE clauses
	if len(whereClauses) > 0 {
		query += " WHERE " + joinStrings(whereClauses, " AND ")
	}

	// Apply OFFSET and LIMIT for row number strategy
	if token.Strategy == SplitStrategyRowNumber {
		if token.Offset > 0 {
			query += fmt.Sprintf(" OFFSET %d", token.Offset)
		}
		if token.Limit > 0 {
			query += fmt.Sprintf(" LIMIT %d", token.Limit)
		}
	}

	return query
}

func (r *Reader) filterColumns(columns []ColumnInfo, projection []string) []ColumnInfo {
	projectionSet := make(map[string]bool)
	for _, col := range projection {
		projectionSet[col] = true
	}

	filtered := make([]ColumnInfo, 0, len(projection))
	for _, col := range columns {
		if projectionSet[col.Name] {
			filtered = append(filtered, col)
		}
	}

	return filtered
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
