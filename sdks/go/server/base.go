package server

import (
	"context"
	"sync"
	"time"

	connectorv1 "github.com/data-power-io/noesis-connectors/sdks/go/gen/connector/v1"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ConnectorHandler defines the interface that connector implementations must provide
type ConnectorHandler interface {
	// CheckConnection validates the connector configuration and connectivity
	CheckConnection(ctx context.Context, config map[string]string) error

	// Discover returns platform information and available entities
	Discover(ctx context.Context, req *connectorv1.DiscoverRequest) (*connectorv1.DiscoverResponse, error)

	// OpenSession creates a new session for data extraction
	OpenSession(ctx context.Context, req *connectorv1.OpenRequest) (string, time.Time, error)

	// CloseSession closes an existing session
	CloseSession(ctx context.Context, sessionID string) error

	// Read streams data according to the specified mode
	Read(ctx context.Context, req *connectorv1.ReadRequest, stream ReadStream) error
}

// ReadStream defines the interface for streaming data back to clients
type ReadStream interface {
	Send(msg *connectorv1.ReadMessage) error
	Context() context.Context
}

// BaseServer provides a base implementation of the Connector gRPC service
type BaseServer struct {
	connectorv1.UnimplementedConnectorServer

	handler   ConnectorHandler
	logger    *zap.Logger
	sessions  map[string]*Session
	sessionMu sync.RWMutex
}

// Session represents an active connector session
type Session struct {
	ID        string
	CreatedAt time.Time
	ExpiresAt time.Time
	Config    map[string]string
}

// NewBaseServer creates a new base connector server
func NewBaseServer(handler ConnectorHandler, logger *zap.Logger) *BaseServer {
	return &BaseServer{
		handler:  handler,
		logger:   logger,
		sessions: make(map[string]*Session),
	}
}

// Check implements the Check RPC method
func (s *BaseServer) Check(ctx context.Context, req *connectorv1.CheckRequest) (*connectorv1.CheckResponse, error) {
	s.logger.Info("Check request received", zap.String("tenant_id", req.TenantId))

	err := s.handler.CheckConnection(ctx, req.Config)
	if err != nil {
		s.logger.Warn("Connection check failed",
			zap.String("tenant_id", req.TenantId),
			zap.Error(err))
		return &connectorv1.CheckResponse{
			Ok:      false,
			Message: err.Error(),
		}, nil
	}

	return &connectorv1.CheckResponse{
		Ok:      true,
		Message: "Connection successful",
	}, nil
}

// Discover implements the Discover RPC method
func (s *BaseServer) Discover(ctx context.Context, req *connectorv1.DiscoverRequest) (*connectorv1.DiscoverResponse, error) {
	s.logger.Info("Discover request received",
		zap.String("tenant_id", req.TenantId),
		zap.Strings("entity_filter", req.EntityFilter),
		zap.Bool("include_schemas", req.IncludeSchemas))

	resp, err := s.handler.Discover(ctx, req)
	if err != nil {
		s.logger.Error("Discovery failed",
			zap.String("tenant_id", req.TenantId),
			zap.Error(err))
		return nil, status.Errorf(codes.Internal, "discovery failed: %v", err)
	}

	return resp, nil
}

// Open implements the Open RPC method
func (s *BaseServer) Open(ctx context.Context, req *connectorv1.OpenRequest) (*connectorv1.OpenResponse, error) {
	s.logger.Info("Open request received", zap.String("tenant_id", req.TenantId))

	sessionID, expiresAt, err := s.handler.OpenSession(ctx, req)
	if err != nil {
		s.logger.Error("Session open failed",
			zap.String("tenant_id", req.TenantId),
			zap.Error(err))
		return nil, status.Errorf(codes.Internal, "failed to open session: %v", err)
	}

	// Store session
	session := &Session{
		ID:        sessionID,
		CreatedAt: time.Now(),
		ExpiresAt: expiresAt,
		Config:    req.Config,
	}

	s.sessionMu.Lock()
	s.sessions[sessionID] = session
	s.sessionMu.Unlock()

	return &connectorv1.OpenResponse{
		SessionId:        sessionID,
		ExpiresAtEpochMs: expiresAt.UnixMilli(),
	}, nil
}

// Close implements the Close RPC method
func (s *BaseServer) Close(ctx context.Context, req *connectorv1.CloseRequest) (*connectorv1.CloseResponse, error) {
	s.logger.Info("Close request received", zap.String("session_id", req.SessionId))

	// Remove session from memory
	s.sessionMu.Lock()
	delete(s.sessions, req.SessionId)
	s.sessionMu.Unlock()

	err := s.handler.CloseSession(ctx, req.SessionId)
	if err != nil {
		s.logger.Warn("Session close failed",
			zap.String("session_id", req.SessionId),
			zap.Error(err))
		// Don't return error as session is already removed from memory
	}

	return &connectorv1.CloseResponse{}, nil
}

// Read implements the Read RPC method (streaming)
func (s *BaseServer) Read(req *connectorv1.ReadRequest, stream connectorv1.Connector_ReadServer) error {
	s.logger.Info("Read request received", zap.String("session_id", req.SessionId))

	// Validate session if provided
	if req.SessionId != "" {
		s.sessionMu.RLock()
		session, exists := s.sessions[req.SessionId]
		s.sessionMu.RUnlock()

		if !exists {
			return status.Errorf(codes.NotFound, "session not found: %s", req.SessionId)
		}

		if time.Now().After(session.ExpiresAt) {
			// Clean up expired session
			s.sessionMu.Lock()
			delete(s.sessions, req.SessionId)
			s.sessionMu.Unlock()

			return status.Errorf(codes.DeadlineExceeded, "session expired: %s", req.SessionId)
		}
	}

	// Wrap the gRPC stream to implement our ReadStream interface
	streamWrapper := &grpcStreamWrapper{stream: stream}

	err := s.handler.Read(stream.Context(), req, streamWrapper)
	if err != nil {
		s.logger.Error("Read operation failed",
			zap.String("session_id", req.SessionId),
			zap.Error(err))
		return status.Errorf(codes.Internal, "read operation failed: %v", err)
	}

	return nil
}

// grpcStreamWrapper adapts the gRPC stream to our ReadStream interface
type grpcStreamWrapper struct {
	stream connectorv1.Connector_ReadServer
}

func (w *grpcStreamWrapper) Send(msg *connectorv1.ReadMessage) error {
	return w.stream.Send(msg)
}

func (w *grpcStreamWrapper) Context() context.Context {
	return w.stream.Context()
}

// GetSession returns session information for a given session ID
func (s *BaseServer) GetSession(sessionID string) (*Session, bool) {
	s.sessionMu.RLock()
	defer s.sessionMu.RUnlock()

	session, exists := s.sessions[sessionID]
	return session, exists
}

// CleanupExpiredSessions removes expired sessions from memory
func (s *BaseServer) CleanupExpiredSessions() {
	s.sessionMu.Lock()
	defer s.sessionMu.Unlock()

	now := time.Now()
	for sessionID, session := range s.sessions {
		if now.After(session.ExpiresAt) {
			delete(s.sessions, sessionID)
			s.logger.Info("Cleaned up expired session", zap.String("session_id", sessionID))
		}
	}
}

// StartSessionCleanup starts a background goroutine to clean up expired sessions
func (s *BaseServer) StartSessionCleanup(interval time.Duration) {
	ticker := time.NewTicker(interval)
	go func() {
		for range ticker.C {
			s.CleanupExpiredSessions()
		}
	}()
}
