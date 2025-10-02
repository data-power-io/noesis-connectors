package postgres

import (
	"context"
	"fmt"
	"strings"
	"time"

	configpkg "github.com/data-power-io/noesis-connectors/connectors/postgres/internal/config"
	"github.com/data-power-io/noesis-connectors/sdks/go/server"
	noesisv1 "github.com/data-power-io/noesis-protocol/languages/go/datapower/noesis/v1"
	"github.com/google/uuid"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Handler struct {
	config map[string]string
	logger *zap.Logger
	client *Client
}

func NewHandler(logger *zap.Logger) (*Handler, error) {
	handler := &Handler{
		logger: logger,
	}

	return handler, nil
}

func (h *Handler) Close() error {
	if h.client != nil {
		h.client.Close()
	}
	return nil
}

func (h *Handler) CheckConnection(ctx context.Context, rawConfig map[string]string) error {
	config, err := configpkg.NormalizeConfig(rawConfig)
	if err != nil {
		return fmt.Errorf("invalid configuration: %w", err)
	}

	h.config = config

	client, err := NewClient(config, h.logger)
	if err != nil {
		return fmt.Errorf("failed to create client: %w", err)
	}
	defer client.Close()

	if err := client.Ping(ctx); err != nil {
		return fmt.Errorf("failed to ping database: %w", err)
	}

	h.logger.Info("PostgreSQL connection check successful")
	return nil
}

func (h *Handler) Discover(ctx context.Context, req *noesisv1.DiscoverRequest) (*noesisv1.DiscoverResponse, error) {
	h.logger.Info("Starting discovery", zap.String("tenant_id", req.TenantId), zap.String("schema", h.config["schema"]))

	// Discovery requires an active session with established client connection
	if h.client == nil {
		return nil, status.Errorf(codes.FailedPrecondition, "no active session - discovery requires an open session")
	}

	client := h.client

	schema := h.config["schema"]

	var entities []*noesisv1.EntityDescriptor

	tables, err := client.GetTables(ctx, schema)
	if err != nil {
		h.logger.Warn("Failed to get tables for schema",
			zap.String("schema", schema),
			zap.Error(err))
	}

	for _, table := range tables {
		columns, err := client.GetColumns(ctx, schema, table.Name)
		if err != nil {
			h.logger.Warn("Failed to get columns for table",
				zap.String("schema", schema),
				zap.String("table", table.Name),
				zap.Error(err))
			continue
		}

		// Get additional metadata
		constraints, err := client.GetConstraints(ctx, schema, table.Name)
		if err != nil {
			h.logger.Warn("Failed to get constraints for table",
				zap.String("schema", schema),
				zap.String("table", table.Name),
				zap.Error(err))
			constraints = []ConstraintInfo{} // Continue with empty constraints
		}

		indexes, err := client.GetIndexes(ctx, schema, table.Name)
		if err != nil {
			h.logger.Warn("Failed to get indexes for table",
				zap.String("schema", schema),
				zap.String("table", table.Name),
				zap.Error(err))
			indexes = []IndexInfo{} // Continue with empty indexes
		}

		tableComment, err := client.GetTableComment(ctx, schema, table.Name)
		if err != nil {
			h.logger.Warn("Failed to get table comment",
				zap.String("schema", schema),
				zap.String("table", table.Name),
				zap.Error(err))
		}

		columnComments, err := client.GetColumnComments(ctx, schema, table.Name)
		if err != nil {
			h.logger.Warn("Failed to get column comments",
				zap.String("schema", schema),
				zap.String("table", table.Name),
				zap.Error(err))
			columnComments = make(map[string]string) // Continue with empty comments
		}

		// Build structured schema with new format
		structuredSchema := buildStructuredSchema(schema, table.Name, columns, constraints, indexes, columnComments)

		// Use table comment from metadata if available, otherwise fallback to table.Comment
		description := table.Comment
		if tableComment != "" {
			description = tableComment
		}

		entity := &noesisv1.EntityDescriptor{
			Name:        table.Name,
			Kind:        noesisv1.EntityKind_NODE, // PostgreSQL tables are NODE entities
			DisplayName: table.Name,
			Description: description,
			Schema:      structuredSchema,
			PrimaryKey:  getPrimaryKeyColumns(columns),
			Capabilities: &noesisv1.ExtractionCapabilities{
				SupportsFullTable:    true,
				SupportsChangeStream: hasTimestampColumn(columns),
				SupportsSubgraph:     false,
			},
		}

		entities = append(entities, entity)
	}

	response := &noesisv1.DiscoverResponse{
		Platform: &noesisv1.PlatformInfo{
			Name:    "PostgreSQL",
			Vendor:  "PostgreSQL Global Development Group",
			Version: "Unknown", // Could query SELECT version() to get actual version
		},
		Entities: entities,
	}

	h.logger.Info("Discovery completed",
		zap.String("tenant_id", req.TenantId),
		zap.String("schema", schema),
		zap.Int("entities", len(entities)))

	// Log response details being sent back to client
	h.logger.Info("Sending discovery response to client",
		zap.String("tenant_id", req.TenantId),
		zap.String("platform_name", response.Platform.Name),
		zap.String("platform_vendor", response.Platform.Vendor),
		zap.String("platform_version", response.Platform.Version),
		zap.Int("total_entities", len(response.Entities)))

	// Log each entity being returned
	for _, entity := range response.Entities {
		// Get schema ID from either format
		schemaId := entity.GetSchema().SchemaId

		h.logger.Debug("Entity details",
			zap.String("entity_name", entity.Name),
			zap.String("entity_kind", entity.Kind.String()),
			zap.String("display_name", entity.DisplayName),
			zap.String("description", entity.Description),
			zap.String("schema_id", schemaId),
			zap.Strings("primary_key", entity.PrimaryKey),
			zap.Bool("supports_full_table", entity.Capabilities.SupportsFullTable),
			zap.Bool("supports_change_stream", entity.Capabilities.SupportsChangeStream),
			zap.Bool("supports_subgraph", entity.Capabilities.SupportsSubgraph))
	}

	return response, nil
}

func (h *Handler) OpenSession(ctx context.Context, req *noesisv1.OpenRequest) (string, time.Time, error) {
	sessionID := uuid.New().String()
	expiresAt := time.Now().Add(time.Hour)

	config, err := configpkg.NormalizeConfig(req.Config)
	if err != nil {
		return "", time.Time{}, status.Errorf(codes.InvalidArgument, "invalid configuration: %v", err)
	}

	client, err := NewClient(config, h.logger)
	if err != nil {
		return "", time.Time{}, status.Errorf(codes.InvalidArgument, "failed to create client: %v", err)
	}

	if err := client.Ping(ctx); err != nil {
		client.Close()
		return "", time.Time{}, status.Errorf(codes.Unavailable, "failed to connect to database: %v", err)
	}

	h.client = client
	h.config = config

	h.logger.Info("Session opened",
		zap.String("session_id", sessionID),
		zap.Time("expires_at", expiresAt))

	return sessionID, expiresAt, nil
}

func (h *Handler) CloseSession(ctx context.Context, sessionID string) error {
	h.logger.Info("Closing session", zap.String("session_id", sessionID))

	if h.client != nil {
		h.client.Close()
		h.client = nil
	}

	return nil
}

func (h *Handler) Read(ctx context.Context, req *noesisv1.ReadRequest, stream server.ReadStream) error {
	h.logger.Info("Starting read operation",
		zap.String("session_id", req.SessionId))

	if h.client == nil {
		return status.Errorf(codes.FailedPrecondition, "no active session")
	}

	reader, err := NewReader(h.client, h.config, h.logger)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to create reader: %v", err)
	}

	return reader.Read(ctx, req, stream)
}

func buildStructuredSchema(schemaName, tableName string, columns []ColumnInfo, constraints []ConstraintInfo, indexes []IndexInfo, columnComments map[string]string) *noesisv1.StructuredSchemaDescriptor {
	// Build field descriptors
	fields := make([]*noesisv1.FieldDescriptor, len(columns))
	for i, col := range columns {
		// Get column comment if available
		documentation := ""
		if comment, exists := columnComments[col.Name]; exists {
			documentation = comment
		}

		// Get default value as string
		defaultValue := ""
		if col.DefaultValue != nil {
			defaultValue = *col.DefaultValue
		}

		fields[i] = &noesisv1.FieldDescriptor{
			Name:            col.Name,
			Type:            mapPostgreSQLTypeToFieldType(col.DataType),
			Nullable:        col.IsNullable == "YES",
			DefaultValue:    defaultValue,
			Documentation:   documentation,
			MaxLength:       int32(col.MaxLength),
			Precision:       int32(col.Precision),
			Scale:           int32(col.Scale),
			OrdinalPosition: int32(col.Position),
			Attributes:      make(map[string]string),
		}

		// Add type-specific attributes
		if col.DataType != "" {
			fields[i].Attributes["postgres_type"] = col.DataType
		}
	}

	// Build constraint descriptors
	constraintDescriptors := make([]*noesisv1.ConstraintDescriptor, len(constraints))
	for i, constraint := range constraints {
		constraintDesc := &noesisv1.ConstraintDescriptor{
			Name:    constraint.Name,
			Type:    mapPostgreSQLConstraintType(constraint.Type),
			Columns: constraint.Columns,
		}

		// Add foreign key specific information
		if constraint.ReferencedTable != nil {
			constraintDesc.ReferencedTable = *constraint.ReferencedTable
		}
		if len(constraint.ReferencedColumns) > 0 {
			constraintDesc.ReferencedColumns = constraint.ReferencedColumns
		}
		if constraint.OnDelete != nil {
			constraintDesc.OnDelete = *constraint.OnDelete
		}
		if constraint.OnUpdate != nil {
			constraintDesc.OnUpdate = *constraint.OnUpdate
		}

		// Add check constraint specific information
		if constraint.CheckExpression != nil {
			constraintDesc.CheckExpression = *constraint.CheckExpression
		}

		// Add documentation
		if constraint.Documentation != nil {
			constraintDesc.Documentation = *constraint.Documentation
		}

		constraintDesc.Attributes = make(map[string]string)
		constraintDesc.Attributes["postgres_constraint_type"] = constraint.Type

		constraintDescriptors[i] = constraintDesc
	}

	// Build index descriptors
	indexDescriptors := make([]*noesisv1.IndexDescriptor, len(indexes))
	for i, index := range indexes {
		indexDesc := &noesisv1.IndexDescriptor{
			Name:    index.Name,
			Columns: index.Columns,
			Unique:  index.IsUnique,
			Type:    index.Type,
		}

		// Add condition expression for partial indexes
		if index.ConditionExpr != nil {
			indexDesc.Condition = *index.ConditionExpr
		}

		// Add documentation
		if index.Documentation != nil {
			indexDesc.Documentation = *index.Documentation
		}

		indexDesc.Attributes = make(map[string]string)
		indexDesc.Attributes["postgres_index_type"] = index.Type

		indexDescriptors[i] = indexDesc
	}

	return &noesisv1.StructuredSchemaDescriptor{
		SchemaId:    fmt.Sprintf("%s_%s_v1", schemaName, tableName),
		Fields:      fields,
		Constraints: constraintDescriptors,
		Indexes:     indexDescriptors,
		Attributes: map[string]string{
			"postgres_schema": schemaName,
			"postgres_table":  tableName,
		},
	}
}

func mapPostgreSQLTypeToFieldType(pgType string) noesisv1.FieldType {
	switch pgType {
	case "integer", "int4":
		return noesisv1.FieldType_FIELD_TYPE_INTEGER
	case "bigint", "int8":
		return noesisv1.FieldType_FIELD_TYPE_BIGINT
	case "smallint", "int2":
		return noesisv1.FieldType_FIELD_TYPE_SMALLINT
	case "real", "float4":
		return noesisv1.FieldType_FIELD_TYPE_FLOAT
	case "double precision", "float8":
		return noesisv1.FieldType_FIELD_TYPE_DOUBLE
	case "boolean", "bool":
		return noesisv1.FieldType_FIELD_TYPE_BOOLEAN
	case "date":
		return noesisv1.FieldType_FIELD_TYPE_DATE
	case "time", "time without time zone":
		return noesisv1.FieldType_FIELD_TYPE_TIME
	case "timestamp", "timestamp without time zone":
		return noesisv1.FieldType_FIELD_TYPE_TIMESTAMP
	case "timestamp with time zone", "timestamptz":
		return noesisv1.FieldType_FIELD_TYPE_TIMESTAMP_WITH_TZ
	case "uuid":
		return noesisv1.FieldType_FIELD_TYPE_UUID
	case "json":
		return noesisv1.FieldType_FIELD_TYPE_JSON
	case "jsonb":
		return noesisv1.FieldType_FIELD_TYPE_JSONB
	case "numeric", "decimal":
		return noesisv1.FieldType_FIELD_TYPE_DECIMAL
	case "bytea":
		return noesisv1.FieldType_FIELD_TYPE_BINARY
	default:
		// Handle character types
		if strings.Contains(pgType, "varchar") || strings.Contains(pgType, "char") {
			return noesisv1.FieldType_FIELD_TYPE_STRING
		}
		// Handle array types
		if strings.Contains(pgType, "[]") {
			return noesisv1.FieldType_FIELD_TYPE_ARRAY
		}
		// Handle enum types (user-defined types are often enums)
		if !strings.Contains(pgType, " ") && pgType != "text" {
			return noesisv1.FieldType_FIELD_TYPE_ENUM
		}
		// Default to text for unknown types
		return noesisv1.FieldType_FIELD_TYPE_TEXT
	}
}

func mapPostgreSQLConstraintType(pgConstraintType string) noesisv1.ConstraintType {
	switch strings.ToUpper(pgConstraintType) {
	case "PRIMARY KEY":
		return noesisv1.ConstraintType_PRIMARY_KEY
	case "FOREIGN KEY":
		return noesisv1.ConstraintType_FOREIGN_KEY
	case "UNIQUE":
		return noesisv1.ConstraintType_UNIQUE
	case "CHECK":
		return noesisv1.ConstraintType_CHECK
	default:
		return noesisv1.ConstraintType_CONSTRAINT_TYPE_UNSPECIFIED
	}
}

func hasTimestampColumn(columns []ColumnInfo) bool {
	for _, col := range columns {
		if col.DataType == "timestamp" ||
			col.DataType == "timestamp without time zone" ||
			col.DataType == "timestamp with time zone" ||
			col.DataType == "timestamptz" ||
			col.Name == "updated_at" ||
			col.Name == "modified_at" {
			return true
		}
	}
	return false
}
func getPrimaryKeyColumns(columns []ColumnInfo) []string {
	// This is a simplified implementation
	// In a real implementation, you would query the information_schema.key_column_usage
	// to get the actual primary key columns
	for _, col := range columns {
		if col.Name == "id" {
			return []string{"id"}
		}
	}
	return []string{}
}
