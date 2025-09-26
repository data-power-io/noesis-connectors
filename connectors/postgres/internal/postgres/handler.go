package postgres

import (
	"context"
	"fmt"
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
	config *configpkg.Config
	logger *zap.Logger
	client *Client
}

func NewHandler(cfg *configpkg.Config, logger *zap.Logger) (*Handler, error) {
	handler := &Handler{
		config: cfg,
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
	h.logger.Info("Starting discovery", zap.String("tenant_id", req.TenantId))

	// Discovery requires an active session with established client connection
	if h.client == nil {
		return nil, status.Errorf(codes.FailedPrecondition, "no active session - discovery requires an open session")
	}

	client := h.client

	schemas, err := client.GetSchemas(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get schemas: %v", err)
	}

	var entities []*noesisv1.EntityDescriptor

	for _, schema := range schemas {
		tables, err := client.GetTables(ctx, schema)
		if err != nil {
			h.logger.Warn("Failed to get tables for schema",
				zap.String("schema", schema),
				zap.Error(err))
			continue
		}

		for _, table := range tables {
			columns, err := client.GetColumns(ctx, table.Schema, table.Name)
			if err != nil {
				h.logger.Warn("Failed to get columns for table",
					zap.String("schema", table.Schema),
					zap.String("table", table.Name),
					zap.Error(err))
				continue
			}

			// Create schema descriptor for the entity
			schemaDesc := &noesisv1.SchemaDescriptor{
				SchemaId: fmt.Sprintf("%s_%s_v1", table.Schema, table.Name),
				// For simplicity, we'll use JSON schema representation
				Spec: &noesisv1.SchemaDescriptor_Json{
					Json: buildJSONSchema(columns),
				},
			}

			entity := &noesisv1.EntityDescriptor{
				Name:        table.Name,
				Kind:        noesisv1.EntityKind_NODE, // PostgreSQL tables are NODE entities
				DisplayName: table.Name,
				Description: table.Comment,
				Schema:      schemaDesc,
				PrimaryKey:  getPrimaryKeyColumns(columns),
				Capabilities: &noesisv1.ExtractionCapabilities{
					SupportsFullTable:    true,
					SupportsChangeStream: hasTimestampColumn(columns),
					SupportsSubgraph:     false,
				},
			}

			entities = append(entities, entity)
		}
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
		zap.Int("schemas", len(schemas)),
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
		h.logger.Debug("Entity details",
			zap.String("entity_name", entity.Name),
			zap.String("entity_kind", entity.Kind.String()),
			zap.String("display_name", entity.DisplayName),
			zap.String("description", entity.Description),
			zap.String("schema_id", entity.Schema.SchemaId),
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

	reader, err := NewReader(h.client, h.logger)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to create reader: %v", err)
	}

	return reader.Read(ctx, req, stream)
}

func mapPostgreSQLTypeToArrow(pgType string) string {
	switch pgType {
	case "integer", "int4":
		return "int32"
	case "bigint", "int8":
		return "int64"
	case "smallint", "int2":
		return "int16"
	case "real", "float4":
		return "float32"
	case "double precision", "float8":
		return "float64"
	case "boolean", "bool":
		return "bool"
	case "date":
		return "date32"
	case "timestamp", "timestamp without time zone":
		return "timestamp[us]"
	case "timestamp with time zone", "timestamptz":
		return "timestamp[us,tz=UTC]"
	case "uuid":
		return "string"
	case "json", "jsonb":
		return "string"
	default:
		return "string"
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

func buildJSONSchema(columns []ColumnInfo) string {
	// Build a simple JSON schema for the table
	schema := `{
		"type": "object",
		"properties": {`

	for i, col := range columns {
		if i > 0 {
			schema += ","
		}
		schema += fmt.Sprintf(`
			"%s": {
				"type": "%s"`, col.Name, mapPostgreSQLTypeToJSONType(col.DataType))

		if col.IsNullable == "YES" {
			schema += `, "nullable": true`
		}

		schema += "}"
	}

	schema += `
		}
	}`

	return schema
}

func mapPostgreSQLTypeToJSONType(pgType string) string {
	switch pgType {
	case "integer", "int4", "bigint", "int8", "smallint", "int2":
		return "integer"
	case "real", "float4", "double precision", "float8":
		return "number"
	case "boolean", "bool":
		return "boolean"
	default:
		return "string"
	}
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
