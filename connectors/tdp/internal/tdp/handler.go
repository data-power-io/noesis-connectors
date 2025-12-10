package tdp

import (
	"context"
	"fmt"
	"time"

	configpkg "github.com/data-power-io/noesis-connectors/connectors/tdp/internal/config"
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
		return fmt.Errorf("failed to connect to S3: %w", err)
	}

	h.logger.Info("TDP connection check successful")
	return nil
}

func (h *Handler) Discover(ctx context.Context, req *noesisv1.DiscoverRequest) (*noesisv1.DiscoverResponse, error) {
	h.logger.Info("Starting discovery", zap.String("tenant_id", req.TenantId))

	if h.client == nil {
		return nil, status.Errorf(codes.FailedPrecondition, "no active session - discovery requires an open session")
	}

	entities := []*noesisv1.EntityDescriptor{
		h.buildBOMManifestEntity(),
		h.buildCADModelsEntity(),
		h.buildDocumentsEntity(),
	}

	response := &noesisv1.DiscoverResponse{
		Platform: &noesisv1.PlatformInfo{
			Name:    "TDP Package",
			Vendor:  "Custom",
			Version: "1.0",
		},
		Entities: entities,
	}

	h.logger.Info("Discovery completed",
		zap.String("tenant_id", req.TenantId),
		zap.Int("entities", len(entities)))

	return response, nil
}

func (h *Handler) buildBOMManifestEntity() *noesisv1.EntityDescriptor {
	fields := []*noesisv1.FieldDescriptor{
		{Name: "level", Type: noesisv1.FieldType_FIELD_TYPE_INTEGER, Nullable: false, OrdinalPosition: 1, Documentation: "BOM hierarchy level (0 = root)"},
		{Name: "parent_pn", Type: noesisv1.FieldType_FIELD_TYPE_STRING, Nullable: true, OrdinalPosition: 2, Documentation: "Parent part number"},
		{Name: "part_number", Type: noesisv1.FieldType_FIELD_TYPE_STRING, Nullable: false, OrdinalPosition: 3, Documentation: "Part number identifier"},
		{Name: "revision", Type: noesisv1.FieldType_FIELD_TYPE_STRING, Nullable: false, OrdinalPosition: 4, Documentation: "Part revision letter"},
		{Name: "nomenclature", Type: noesisv1.FieldType_FIELD_TYPE_STRING, Nullable: false, OrdinalPosition: 5, Documentation: "Part description/name"},
		{Name: "qty", Type: noesisv1.FieldType_FIELD_TYPE_INTEGER, Nullable: false, OrdinalPosition: 6, Documentation: "Quantity required"},
		{Name: "type", Type: noesisv1.FieldType_FIELD_TYPE_STRING, Nullable: false, OrdinalPosition: 7, Documentation: "Part type (Assembly, Component, Standard)"},
		{Name: "material", Type: noesisv1.FieldType_FIELD_TYPE_STRING, Nullable: true, OrdinalPosition: 8, Documentation: "Material specification"},
		{Name: "weight_kg", Type: noesisv1.FieldType_FIELD_TYPE_DOUBLE, Nullable: true, OrdinalPosition: 9, Documentation: "Weight in kilograms"},
		{Name: "cage_code", Type: noesisv1.FieldType_FIELD_TYPE_STRING, Nullable: true, OrdinalPosition: 10, Documentation: "Commercial and Government Entity Code"},
		{Name: "cad_file", Type: noesisv1.FieldType_FIELD_TYPE_STRING, Nullable: true, OrdinalPosition: 11, Documentation: "Referenced CAD file name"},
		{Name: "linked_document", Type: noesisv1.FieldType_FIELD_TYPE_STRING, Nullable: true, OrdinalPosition: 12, Documentation: "Referenced document file name"},
		{Name: "document_type", Type: noesisv1.FieldType_FIELD_TYPE_STRING, Nullable: true, OrdinalPosition: 13, Documentation: "Type of linked document"},
	}

	return &noesisv1.EntityDescriptor{
		Name:        string(EntityBOMManifest),
		Kind:        noesisv1.EntityKind_NODE,
		DisplayName: "BOM Manifest",
		Description: "Bill of Materials manifest from the TDP package CSV file",
		Schema: &noesisv1.StructuredSchemaDescriptor{
			SchemaId: "bom_manifest_v1",
			Fields:   fields,
			Attributes: map[string]string{
				"source_file": "BOM_Manifest_With_Docs.csv",
			},
		},
		PrimaryKey: []string{"part_number"},
		Capabilities: &noesisv1.ExtractionCapabilities{
			SupportsFullTable:    true,
			SupportsChangeStream: false,
			SupportsSubgraph:     false,
		},
	}
}

func (h *Handler) buildCADModelsEntity() *noesisv1.EntityDescriptor {
	fields := []*noesisv1.FieldDescriptor{
		{Name: "path", Type: noesisv1.FieldType_FIELD_TYPE_STRING, Nullable: false, OrdinalPosition: 1, Documentation: "Full S3 path to the file"},
		{Name: "name", Type: noesisv1.FieldType_FIELD_TYPE_STRING, Nullable: false, OrdinalPosition: 2, Documentation: "File name"},
		{Name: "size", Type: noesisv1.FieldType_FIELD_TYPE_BIGINT, Nullable: false, OrdinalPosition: 3, Documentation: "File size in bytes"},
		{Name: "modified_time", Type: noesisv1.FieldType_FIELD_TYPE_TIMESTAMP, Nullable: false, OrdinalPosition: 4, Documentation: "Last modified timestamp"},
		{Name: "extension", Type: noesisv1.FieldType_FIELD_TYPE_STRING, Nullable: false, OrdinalPosition: 5, Documentation: "File extension"},
	}

	return &noesisv1.EntityDescriptor{
		Name:        string(EntityCADModels),
		Kind:        noesisv1.EntityKind_NODE,
		DisplayName: "CAD Models",
		Description: "STEP CAD file metadata from the 3D_Models directory",
		Schema: &noesisv1.StructuredSchemaDescriptor{
			SchemaId: "cad_models_v1",
			Fields:   fields,
			Attributes: map[string]string{
				"source_directory": "3D_Models",
				"file_types":       ".step,.stp",
			},
		},
		PrimaryKey: []string{"path"},
		Capabilities: &noesisv1.ExtractionCapabilities{
			SupportsFullTable:    true,
			SupportsChangeStream: false,
			SupportsSubgraph:     false,
		},
	}
}

func (h *Handler) buildDocumentsEntity() *noesisv1.EntityDescriptor {
	fields := []*noesisv1.FieldDescriptor{
		{Name: "path", Type: noesisv1.FieldType_FIELD_TYPE_STRING, Nullable: false, OrdinalPosition: 1, Documentation: "Full S3 path to the file"},
		{Name: "name", Type: noesisv1.FieldType_FIELD_TYPE_STRING, Nullable: false, OrdinalPosition: 2, Documentation: "File name"},
		{Name: "size", Type: noesisv1.FieldType_FIELD_TYPE_BIGINT, Nullable: false, OrdinalPosition: 3, Documentation: "File size in bytes"},
		{Name: "modified_time", Type: noesisv1.FieldType_FIELD_TYPE_TIMESTAMP, Nullable: false, OrdinalPosition: 4, Documentation: "Last modified timestamp"},
		{Name: "extension", Type: noesisv1.FieldType_FIELD_TYPE_STRING, Nullable: false, OrdinalPosition: 5, Documentation: "File extension"},
	}

	return &noesisv1.EntityDescriptor{
		Name:        string(EntityDocuments),
		Kind:        noesisv1.EntityKind_NODE,
		DisplayName: "Documents",
		Description: "PDF document metadata from the Documents directory",
		Schema: &noesisv1.StructuredSchemaDescriptor{
			SchemaId: "documents_v1",
			Fields:   fields,
			Attributes: map[string]string{
				"source_directory": "Documents",
				"file_types":       ".pdf",
			},
		},
		PrimaryKey: []string{"path"},
		Capabilities: &noesisv1.ExtractionCapabilities{
			SupportsFullTable:    true,
			SupportsChangeStream: false,
			SupportsSubgraph:     false,
		},
	}
}

func (h *Handler) PlanExtraction(ctx context.Context, req *noesisv1.PlanExtractionRequest) (*noesisv1.PlanExtractionResponse, error) {
	h.logger.Info("Planning extraction",
		zap.String("entity", req.Entity),
		zap.Int32("desired_parallelism", req.DesiredParallelism))

	// Normalize config
	config, err := configpkg.NormalizeConfig(req.Config)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid configuration: %v", err)
	}

	// Create client for row count estimation
	client, err := NewClient(config, h.logger)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "failed to create client: %v", err)
	}
	defer client.Close()

	// Get estimated row counts
	counts, err := client.GetRowCounts(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get row counts: %v", err)
	}

	entityType := EntityType(req.Entity)
	rowCount, exists := counts[entityType]
	if !exists {
		return nil, status.Errorf(codes.NotFound, "unknown entity: %s", req.Entity)
	}

	// TDP packages are typically small, so we use a single split
	split := &noesisv1.ExtractionSplit{
		SplitId:       "split-0000",
		SplitToken:    []byte(fmt.Sprintf(`{"entity":"%s","index":0}`, req.Entity)),
		EstimatedRows: rowCount,
		Metadata: map[string]string{
			"entity":      req.Entity,
			"split_index": "0",
		},
	}

	h.logger.Info("Extraction plan generated",
		zap.String("entity", req.Entity),
		zap.Int64("estimated_rows", rowCount))

	return &noesisv1.PlanExtractionResponse{
		Splits:             []*noesisv1.ExtractionSplit{split},
		TotalEstimatedRows: rowCount,
	}, nil
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
		return "", time.Time{}, status.Errorf(codes.Unavailable, "failed to connect to S3: %v", err)
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
	h.logger.Info("Starting read operation", zap.String("session_id", req.SessionId))

	if h.client == nil {
		return status.Errorf(codes.FailedPrecondition, "no active session")
	}

	reader, err := NewReader(h.client, h.config, h.logger)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to create reader: %v", err)
	}

	return reader.Read(ctx, req, stream)
}

func (h *Handler) ReadSplit(ctx context.Context, req *noesisv1.ReadSplitRequest, stream server.ReadStream) error {
	h.logger.Info("Starting read split operation",
		zap.String("entity", req.Entity),
		zap.String("split_id", req.Split.GetSplitId()))

	if h.client == nil {
		return status.Errorf(codes.FailedPrecondition, "no active session - please call Open first")
	}

	reader, err := NewReader(h.client, h.config, h.logger)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to create reader: %v", err)
	}

	return reader.ReadSplit(ctx, req, stream)
}
