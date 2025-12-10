package tdp

import (
	"context"
	"strconv"

	"github.com/data-power-io/noesis-connectors/sdks/go/server"
	noesisv1 "github.com/data-power-io/noesis-protocol/languages/go/datapower/noesis/v1"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
	fullTable := req.GetFullTable()
	if fullTable == nil {
		return status.Errorf(codes.InvalidArgument, "only full table extraction is supported for TDP")
	}

	entity := fullTable.Entity
	return r.readEntity(ctx, EntityType(entity), stream)
}

func (r *Reader) ReadSplit(ctx context.Context, req *noesisv1.ReadSplitRequest, stream server.ReadStream) error {
	entity := req.Entity
	return r.readEntity(ctx, EntityType(entity), stream)
}

func (r *Reader) readEntity(ctx context.Context, entity EntityType, stream server.ReadStream) error {
	r.logger.Info("Reading entity", zap.String("entity", string(entity)))

	var recordCount int
	var err error

	switch entity {
	case EntityBOMManifest:
		recordCount, err = r.streamBOMManifest(ctx, stream)
	case EntityCADModels:
		recordCount, err = r.streamCADModels(ctx, stream)
	case EntityDocuments:
		recordCount, err = r.streamDocuments(ctx, stream)
	default:
		return status.Errorf(codes.NotFound, "unknown entity: %s", entity)
	}

	if err != nil {
		return err
	}

	// Send state message with cursor
	stateMsg := &noesisv1.ReadMessage{
		Msg: &noesisv1.ReadMessage_State{
			State: &noesisv1.StateMsg{
				Cursor: &noesisv1.Cursor{
					Token: []byte(strconv.Itoa(recordCount)),
				},
			},
		},
	}

	if err := stream.Send(stateMsg); err != nil {
		return status.Errorf(codes.Internal, "failed to send state: %v", err)
	}

	r.logger.Info("Completed streaming", zap.String("entity", string(entity)), zap.Int("records", recordCount))
	return nil
}

func (r *Reader) streamBOMManifest(ctx context.Context, stream server.ReadStream) (int, error) {
	records, err := r.client.GetBOMManifest(ctx)
	if err != nil {
		return 0, status.Errorf(codes.Internal, "failed to read BOM manifest: %v", err)
	}

	for i, rec := range records {
		record := r.bomRecordToProto(rec)
		msg := &noesisv1.ReadMessage{
			Msg: &noesisv1.ReadMessage_Record{
				Record: record,
			},
		}

		if err := stream.Send(msg); err != nil {
			return i, status.Errorf(codes.Internal, "failed to send record: %v", err)
		}

		// Check for cancellation periodically
		if i%100 == 0 {
			select {
			case <-ctx.Done():
				return i, ctx.Err()
			default:
			}
		}
	}

	return len(records), nil
}

func (r *Reader) bomRecordToProto(rec BOMRecord) *noesisv1.RecordMsg {
	columns := map[string]*noesisv1.Value{
		"level":           {Kind: &noesisv1.Value_Int32Val{Int32Val: int32(rec.Level)}},
		"parent_pn":       r.stringOrNull(rec.ParentPN),
		"part_number":     {Kind: &noesisv1.Value_StringVal{StringVal: rec.PartNumber}},
		"revision":        {Kind: &noesisv1.Value_StringVal{StringVal: rec.Revision}},
		"nomenclature":    {Kind: &noesisv1.Value_StringVal{StringVal: rec.Nomenclature}},
		"qty":             {Kind: &noesisv1.Value_Int32Val{Int32Val: int32(rec.Qty)}},
		"type":            {Kind: &noesisv1.Value_StringVal{StringVal: rec.Type}},
		"material":        r.stringOrNull(rec.Material),
		"weight_kg":       {Kind: &noesisv1.Value_DoubleVal{DoubleVal: rec.WeightKg}},
		"cage_code":       r.stringOrNull(rec.CAGECode),
		"cad_file":        r.stringOrNull(rec.CADFile),
		"linked_document": r.stringOrNull(rec.LinkedDocument),
		"document_type":   r.stringOrNull(rec.DocumentType),
	}

	return &noesisv1.RecordMsg{
		Entity: string(EntityBOMManifest),
		Op:     noesisv1.Op_UPSERT,
		Data: &noesisv1.Row{
			Columns: columns,
		},
	}
}

func (r *Reader) streamCADModels(ctx context.Context, stream server.ReadStream) (int, error) {
	files, err := r.client.ListCADModels(ctx)
	if err != nil {
		return 0, status.Errorf(codes.Internal, "failed to list CAD models: %v", err)
	}

	for i, file := range files {
		record := r.fileMetadataToProto(file, EntityCADModels)
		msg := &noesisv1.ReadMessage{
			Msg: &noesisv1.ReadMessage_Record{
				Record: record,
			},
		}

		if err := stream.Send(msg); err != nil {
			return i, status.Errorf(codes.Internal, "failed to send record: %v", err)
		}

		// Check for cancellation periodically
		if i%100 == 0 {
			select {
			case <-ctx.Done():
				return i, ctx.Err()
			default:
			}
		}
	}

	return len(files), nil
}

func (r *Reader) streamDocuments(ctx context.Context, stream server.ReadStream) (int, error) {
	files, err := r.client.ListDocuments(ctx)
	if err != nil {
		return 0, status.Errorf(codes.Internal, "failed to list documents: %v", err)
	}

	for i, file := range files {
		record := r.fileMetadataToProto(file, EntityDocuments)
		msg := &noesisv1.ReadMessage{
			Msg: &noesisv1.ReadMessage_Record{
				Record: record,
			},
		}

		if err := stream.Send(msg); err != nil {
			return i, status.Errorf(codes.Internal, "failed to send record: %v", err)
		}

		// Check for cancellation periodically
		if i%100 == 0 {
			select {
			case <-ctx.Done():
				return i, ctx.Err()
			default:
			}
		}
	}

	return len(files), nil
}

func (r *Reader) fileMetadataToProto(file FileMetadata, entity EntityType) *noesisv1.RecordMsg {
	columns := map[string]*noesisv1.Value{
		"path":          {Kind: &noesisv1.Value_StringVal{StringVal: file.Path}},
		"name":          {Kind: &noesisv1.Value_StringVal{StringVal: file.Name}},
		"size":          {Kind: &noesisv1.Value_Int64Val{Int64Val: file.Size}},
		"modified_time": {Kind: &noesisv1.Value_TimestampMicros{TimestampMicros: file.ModifiedTime}},
		"extension":     {Kind: &noesisv1.Value_StringVal{StringVal: file.Extension}},
	}

	return &noesisv1.RecordMsg{
		Entity: string(entity),
		Op:     noesisv1.Op_UPSERT,
		Data: &noesisv1.Row{
			Columns: columns,
		},
	}
}

func (r *Reader) stringOrNull(s string) *noesisv1.Value {
	if s == "" {
		return &noesisv1.Value{
			Kind: &noesisv1.Value_NullVal{NullVal: noesisv1.NullValue_NULL_VALUE},
		}
	}
	return &noesisv1.Value{
		Kind: &noesisv1.Value_StringVal{StringVal: s},
	}
}
