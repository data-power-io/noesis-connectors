package cursor

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"

	connectorv1 "github.com/data-power-io/noesis-protocol/languages/go/datapower/noesis/v1"
)

// Manager provides utilities for cursor management
type Manager struct{}

// NewManager creates a new cursor manager
func NewManager() *Manager {
	return &Manager{}
}

// Cursor represents an opaque cursor for resumable operations
type Cursor struct {
	Type      string                 `json:"type"`
	Position  interface{}            `json:"position"`
	Metadata  map[string]interface{} `json:"metadata,omitempty"`
	Timestamp time.Time              `json:"timestamp"`
}

// CreateCursor creates a new cursor from position data
func (m *Manager) CreateCursor(cursorType string, position interface{}, metadata map[string]interface{}) (*connectorv1.Cursor, error) {
	cursor := &Cursor{
		Type:      cursorType,
		Position:  position,
		Metadata:  metadata,
		Timestamp: time.Now(),
	}

	data, err := json.Marshal(cursor)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal cursor: %w", err)
	}

	// Encode as base64 for opaque handling
	token := base64.URLEncoding.EncodeToString(data)

	return &connectorv1.Cursor{
		Token: []byte(token),
	}, nil
}

// ParseCursor parses an opaque cursor back to structured data
func (m *Manager) ParseCursor(cursor *connectorv1.Cursor) (*Cursor, error) {
	if cursor == nil || len(cursor.Token) == 0 {
		return nil, nil
	}

	// Decode from base64
	data, err := base64.URLEncoding.DecodeString(string(cursor.Token))
	if err != nil {
		return nil, fmt.Errorf("failed to decode cursor token: %w", err)
	}

	var parsed Cursor
	if err := json.Unmarshal(data, &parsed); err != nil {
		return nil, fmt.Errorf("failed to unmarshal cursor: %w", err)
	}

	return &parsed, nil
}

// OffsetCursor represents a simple offset-based cursor
type OffsetCursor struct {
	Offset int64 `json:"offset"`
	Limit  int64 `json:"limit,omitempty"`
}

// CreateOffsetCursor creates an offset-based cursor
func (m *Manager) CreateOffsetCursor(offset, limit int64) (*connectorv1.Cursor, error) {
	return m.CreateCursor("offset", &OffsetCursor{
		Offset: offset,
		Limit:  limit,
	}, nil)
}

// ParseOffsetCursor parses an offset cursor
func (m *Manager) ParseOffsetCursor(cursor *connectorv1.Cursor) (*OffsetCursor, error) {
	parsed, err := m.ParseCursor(cursor)
	if err != nil {
		return nil, err
	}

	if parsed == nil {
		return &OffsetCursor{Offset: 0}, nil
	}

	if parsed.Type != "offset" {
		return nil, fmt.Errorf("expected offset cursor, got %s", parsed.Type)
	}

	// Convert position back to OffsetCursor
	positionBytes, err := json.Marshal(parsed.Position)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal position: %w", err)
	}

	var offsetCursor OffsetCursor
	if err := json.Unmarshal(positionBytes, &offsetCursor); err != nil {
		return nil, fmt.Errorf("failed to unmarshal offset cursor: %w", err)
	}

	return &offsetCursor, nil
}

// TimestampCursor represents a timestamp-based cursor
type TimestampCursor struct {
	Timestamp time.Time `json:"timestamp"`
	LastID    string    `json:"last_id,omitempty"`
}

// CreateTimestampCursor creates a timestamp-based cursor
func (m *Manager) CreateTimestampCursor(timestamp time.Time, lastID string) (*connectorv1.Cursor, error) {
	return m.CreateCursor("timestamp", &TimestampCursor{
		Timestamp: timestamp,
		LastID:    lastID,
	}, nil)
}

// ParseTimestampCursor parses a timestamp cursor
func (m *Manager) ParseTimestampCursor(cursor *connectorv1.Cursor) (*TimestampCursor, error) {
	parsed, err := m.ParseCursor(cursor)
	if err != nil {
		return nil, err
	}

	if parsed == nil {
		return &TimestampCursor{}, nil
	}

	if parsed.Type != "timestamp" {
		return nil, fmt.Errorf("expected timestamp cursor, got %s", parsed.Type)
	}

	// Convert position back to TimestampCursor
	positionBytes, err := json.Marshal(parsed.Position)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal position: %w", err)
	}

	var timestampCursor TimestampCursor
	if err := json.Unmarshal(positionBytes, &timestampCursor); err != nil {
		return nil, fmt.Errorf("failed to unmarshal timestamp cursor: %w", err)
	}

	return &timestampCursor, nil
}

// LSNCursor represents a Log Sequence Number cursor (for database CDC)
type LSNCursor struct {
	LSN      string            `json:"lsn"`
	Database string            `json:"database,omitempty"`
	Metadata map[string]string `json:"metadata,omitempty"`
}

// CreateLSNCursor creates an LSN-based cursor
func (m *Manager) CreateLSNCursor(lsn, database string, metadata map[string]string) (*connectorv1.Cursor, error) {
	metadataIface := make(map[string]interface{})
	for k, v := range metadata {
		metadataIface[k] = v
	}

	return m.CreateCursor("lsn", &LSNCursor{
		LSN:      lsn,
		Database: database,
		Metadata: metadata,
	}, metadataIface)
}

// ParseLSNCursor parses an LSN cursor
func (m *Manager) ParseLSNCursor(cursor *connectorv1.Cursor) (*LSNCursor, error) {
	parsed, err := m.ParseCursor(cursor)
	if err != nil {
		return nil, err
	}

	if parsed == nil {
		return &LSNCursor{}, nil
	}

	if parsed.Type != "lsn" {
		return nil, fmt.Errorf("expected LSN cursor, got %s", parsed.Type)
	}

	// Convert position back to LSNCursor
	positionBytes, err := json.Marshal(parsed.Position)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal position: %w", err)
	}

	var lsnCursor LSNCursor
	if err := json.Unmarshal(positionBytes, &lsnCursor); err != nil {
		return nil, fmt.Errorf("failed to unmarshal LSN cursor: %w", err)
	}

	return &lsnCursor, nil
}

// SubgraphCursor represents a cursor for subgraph traversal
type SubgraphCursor struct {
	VisitedNodes map[string]bool `json:"visited_nodes"`
	CurrentDepth int             `json:"current_depth"`
	Frontier     []TraversalNode `json:"frontier"`
	Completed    []string        `json:"completed_groups,omitempty"`
}

// TraversalNode represents a node in the traversal frontier
type TraversalNode struct {
	EntityName string `json:"entity_name"`
	EntityKey  string `json:"entity_key"`
	Depth      int    `json:"depth"`
	GroupID    string `json:"group_id,omitempty"`
}

// CreateSubgraphCursor creates a subgraph traversal cursor
func (m *Manager) CreateSubgraphCursor(visitedNodes map[string]bool, currentDepth int, frontier []TraversalNode, completed []string) (*connectorv1.Cursor, error) {
	return m.CreateCursor("subgraph", &SubgraphCursor{
		VisitedNodes: visitedNodes,
		CurrentDepth: currentDepth,
		Frontier:     frontier,
		Completed:    completed,
	}, nil)
}

// ParseSubgraphCursor parses a subgraph cursor
func (m *Manager) ParseSubgraphCursor(cursor *connectorv1.Cursor) (*SubgraphCursor, error) {
	parsed, err := m.ParseCursor(cursor)
	if err != nil {
		return nil, err
	}

	if parsed == nil {
		return &SubgraphCursor{
			VisitedNodes: make(map[string]bool),
			CurrentDepth: 0,
			Frontier:     []TraversalNode{},
			Completed:    []string{},
		}, nil
	}

	if parsed.Type != "subgraph" {
		return nil, fmt.Errorf("expected subgraph cursor, got %s", parsed.Type)
	}

	// Convert position back to SubgraphCursor
	positionBytes, err := json.Marshal(parsed.Position)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal position: %w", err)
	}

	var subgraphCursor SubgraphCursor
	if err := json.Unmarshal(positionBytes, &subgraphCursor); err != nil {
		return nil, fmt.Errorf("failed to unmarshal subgraph cursor: %w", err)
	}

	// Initialize maps if nil
	if subgraphCursor.VisitedNodes == nil {
		subgraphCursor.VisitedNodes = make(map[string]bool)
	}
	if subgraphCursor.Frontier == nil {
		subgraphCursor.Frontier = []TraversalNode{}
	}
	if subgraphCursor.Completed == nil {
		subgraphCursor.Completed = []string{}
	}

	return &subgraphCursor, nil
}

// ValidateCursor validates that a cursor is well-formed
func (m *Manager) ValidateCursor(cursor *connectorv1.Cursor) error {
	if cursor == nil {
		return nil // nil cursor is valid (represents start position)
	}

	parsed, err := m.ParseCursor(cursor)
	if err != nil {
		return fmt.Errorf("invalid cursor: %w", err)
	}

	// Check if cursor is not too old (prevent replay attacks)
	if time.Since(parsed.Timestamp) > 24*time.Hour {
		return fmt.Errorf("cursor is too old")
	}

	return nil
}

// CursorAge returns the age of a cursor
func (m *Manager) CursorAge(cursor *connectorv1.Cursor) (time.Duration, error) {
	if cursor == nil {
		return 0, nil
	}

	parsed, err := m.ParseCursor(cursor)
	if err != nil {
		return 0, err
	}

	return time.Since(parsed.Timestamp), nil
}
