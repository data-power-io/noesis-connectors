package client

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	connectorv1 "github.com/data-power-io/noesis-protocol/languages/go/datapower/noesis/v1"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"
)

// GRPCClient provides a client for communicating with connector gRPC services
type GRPCClient struct {
	conn     *grpc.ClientConn
	client   connectorv1.ConnectorClient
	logger   *zap.Logger
	address  string
	sessions map[string]*SessionInfo
	mu       sync.RWMutex

	// Connection pool settings
	maxRetries     int
	retryDelay     time.Duration
	connectionPool *ConnectionPool
}

// SessionInfo holds information about active sessions
type SessionInfo struct {
	ID        string
	CreatedAt time.Time
	ExpiresAt time.Time
}

// ConnectionPool manages gRPC connection pooling
type ConnectionPool struct {
	connections map[string]*grpc.ClientConn
	mu          sync.RWMutex
	maxIdle     time.Duration
	maxConns    int
}

// ClientConfig holds configuration for the gRPC client
type ClientConfig struct {
	Address           string
	MaxRetries        int
	RetryDelay        time.Duration
	ConnectionTimeout time.Duration
	KeepAliveTime     time.Duration
	KeepAliveTimeout  time.Duration
	MaxPoolSize       int
	MaxIdleTime       time.Duration
}

// DefaultClientConfig returns a default client configuration
func DefaultClientConfig(address string) *ClientConfig {
	return &ClientConfig{
		Address:           address,
		MaxRetries:        3,
		RetryDelay:        time.Second,
		ConnectionTimeout: 30 * time.Second,
		KeepAliveTime:     30 * time.Second,
		KeepAliveTimeout:  5 * time.Second,
		MaxPoolSize:       10,
		MaxIdleTime:       5 * time.Minute,
	}
}

// NewGRPCClient creates a new gRPC client for connector communication
func NewGRPCClient(config *ClientConfig, logger *zap.Logger) (*GRPCClient, error) {
	// Configure connection options
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()), // TODO: Add TLS support
		grpc.WithBlock(),
		grpc.WithTimeout(config.ConnectionTimeout),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                config.KeepAliveTime,
			Timeout:             config.KeepAliveTimeout,
			PermitWithoutStream: true,
		}),
		// Add interceptors for retries and logging
		grpc.WithUnaryInterceptor(retryInterceptor(config.MaxRetries, config.RetryDelay, logger)),
		grpc.WithStreamInterceptor(streamRetryInterceptor(config.MaxRetries, config.RetryDelay, logger)),
	}

	conn, err := grpc.Dial(config.Address, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to connector at %s: %w", config.Address, err)
	}

	client := connectorv1.NewConnectorClient(conn)

	pool := &ConnectionPool{
		connections: make(map[string]*grpc.ClientConn),
		maxIdle:     config.MaxIdleTime,
		maxConns:    config.MaxPoolSize,
	}

	return &GRPCClient{
		conn:           conn,
		client:         client,
		logger:         logger,
		address:        config.Address,
		sessions:       make(map[string]*SessionInfo),
		maxRetries:     config.MaxRetries,
		retryDelay:     config.RetryDelay,
		connectionPool: pool,
	}, nil
}

// Check tests the connection to the connector
func (c *GRPCClient) Check(ctx context.Context, tenantID string, config map[string]string) (*connectorv1.CheckResponse, error) {
	req := &connectorv1.CheckRequest{
		TenantId: tenantID,
		Config:   config,
	}

	resp, err := c.client.Check(ctx, req)
	if err != nil {
		c.logger.Error("Check request failed",
			zap.String("address", c.address),
			zap.String("tenant_id", tenantID),
			zap.Error(err))
		return nil, err
	}

	c.logger.Info("Check request completed",
		zap.String("address", c.address),
		zap.String("tenant_id", tenantID),
		zap.Bool("ok", resp.Ok))

	return resp, nil
}

// Discover retrieves platform and entity information from the connector
func (c *GRPCClient) Discover(ctx context.Context, req *connectorv1.DiscoverRequest) (*connectorv1.DiscoverResponse, error) {
	resp, err := c.client.Discover(ctx, req)
	if err != nil {
		c.logger.Error("Discover request failed",
			zap.String("address", c.address),
			zap.String("tenant_id", req.TenantId),
			zap.Error(err))
		return nil, err
	}

	c.logger.Info("Discover request completed",
		zap.String("address", c.address),
		zap.String("tenant_id", req.TenantId),
		zap.Int("entities_count", len(resp.Entities)))

	return resp, nil
}

// Open creates a new session with the connector
func (c *GRPCClient) Open(ctx context.Context, req *connectorv1.OpenRequest) (*connectorv1.OpenResponse, error) {
	resp, err := c.client.Open(ctx, req)
	if err != nil {
		c.logger.Error("Open request failed",
			zap.String("address", c.address),
			zap.String("tenant_id", req.TenantId),
			zap.Error(err))
		return nil, err
	}

	// Store session info
	sessionInfo := &SessionInfo{
		ID:        resp.SessionId,
		CreatedAt: time.Now(),
		ExpiresAt: time.UnixMilli(resp.ExpiresAtEpochMs),
	}

	c.mu.Lock()
	c.sessions[resp.SessionId] = sessionInfo
	c.mu.Unlock()

	c.logger.Info("Session opened",
		zap.String("address", c.address),
		zap.String("session_id", resp.SessionId),
		zap.Time("expires_at", sessionInfo.ExpiresAt))

	return resp, nil
}

// Close closes a session with the connector
func (c *GRPCClient) Close(ctx context.Context, sessionID string) error {
	req := &connectorv1.CloseRequest{
		SessionId: sessionID,
	}

	_, err := c.client.Close(ctx, req)
	if err != nil {
		c.logger.Error("Close request failed",
			zap.String("address", c.address),
			zap.String("session_id", sessionID),
			zap.Error(err))
		return err
	}

	// Remove session info
	c.mu.Lock()
	delete(c.sessions, sessionID)
	c.mu.Unlock()

	c.logger.Info("Session closed",
		zap.String("address", c.address),
		zap.String("session_id", sessionID))

	return nil
}

// Read starts streaming data from the connector
func (c *GRPCClient) Read(ctx context.Context, req *connectorv1.ReadRequest, handler MessageHandler) error {
	stream, err := c.client.Read(ctx, req)
	if err != nil {
		c.logger.Error("Read stream failed to start",
			zap.String("address", c.address),
			zap.String("session_id", req.SessionId),
			zap.Error(err))
		return err
	}

	c.logger.Info("Read stream started",
		zap.String("address", c.address),
		zap.String("session_id", req.SessionId))

	// Process streaming messages
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			c.logger.Info("Read stream completed",
				zap.String("address", c.address),
				zap.String("session_id", req.SessionId))
			break
		}
		if err != nil {
			c.logger.Error("Read stream error",
				zap.String("address", c.address),
				zap.String("session_id", req.SessionId),
				zap.Error(err))
			return err
		}

		// Handle the message
		if err := handler.HandleMessage(msg); err != nil {
			c.logger.Error("Message handler error",
				zap.String("address", c.address),
				zap.String("session_id", req.SessionId),
				zap.Error(err))
			return err
		}
	}

	return nil
}

// MessageHandler defines the interface for handling streaming messages
type MessageHandler interface {
	HandleMessage(msg *connectorv1.ReadMessage) error
}

// MessageHandlerFunc is a function adapter for MessageHandler
type MessageHandlerFunc func(msg *connectorv1.ReadMessage) error

func (f MessageHandlerFunc) HandleMessage(msg *connectorv1.ReadMessage) error {
	return f(msg)
}

// GetSessionInfo returns session information
func (c *GRPCClient) GetSessionInfo(sessionID string) (*SessionInfo, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	session, exists := c.sessions[sessionID]
	return session, exists
}

// CleanupExpiredSessions removes expired sessions from local tracking
func (c *GRPCClient) CleanupExpiredSessions() {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()
	for sessionID, session := range c.sessions {
		if now.After(session.ExpiresAt) {
			delete(c.sessions, sessionID)
			c.logger.Info("Cleaned up expired session", zap.String("session_id", sessionID))
		}
	}
}

// Close closes the gRPC connection
func (c *GRPCClient) CloseConnection() error {
	return c.conn.Close()
}

// IsHealthy checks if the connection is healthy
func (c *GRPCClient) IsHealthy() bool {
	state := c.conn.GetState()
	return state == connectivity.Ready
}

// retryInterceptor implements retry logic for unary calls
func retryInterceptor(maxRetries int, retryDelay time.Duration, logger *zap.Logger) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		var err error
		for attempt := 0; attempt <= maxRetries; attempt++ {
			err = invoker(ctx, method, req, reply, cc, opts...)
			if err == nil {
				return nil
			}

			// Check if error is retryable
			if !isRetryableError(err) {
				return err
			}

			if attempt < maxRetries {
				logger.Warn("Retrying gRPC call",
					zap.String("method", method),
					zap.Int("attempt", attempt+1),
					zap.Int("max_retries", maxRetries),
					zap.Error(err))

				select {
				case <-time.After(retryDelay * time.Duration(attempt+1)):
					// Continue to next attempt
				case <-ctx.Done():
					return ctx.Err()
				}
			}
		}

		return err
	}
}

// streamRetryInterceptor implements retry logic for streaming calls
func streamRetryInterceptor(maxRetries int, retryDelay time.Duration, logger *zap.Logger) grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		var stream grpc.ClientStream
		var err error

		for attempt := 0; attempt <= maxRetries; attempt++ {
			stream, err = streamer(ctx, desc, cc, method, opts...)
			if err == nil {
				return stream, nil
			}

			// Check if error is retryable
			if !isRetryableError(err) {
				return nil, err
			}

			if attempt < maxRetries {
				logger.Warn("Retrying gRPC stream",
					zap.String("method", method),
					zap.Int("attempt", attempt+1),
					zap.Int("max_retries", maxRetries),
					zap.Error(err))

				select {
				case <-time.After(retryDelay * time.Duration(attempt+1)):
					// Continue to next attempt
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}
		}

		return nil, err
	}
}

// isRetryableError determines if an error is retryable
func isRetryableError(err error) bool {
	if err == nil {
		return false
	}

	st, ok := status.FromError(err)
	if !ok {
		return false
	}

	switch st.Code() {
	case codes.Unavailable, codes.DeadlineExceeded, codes.ResourceExhausted:
		return true
	case codes.Internal:
		// Some internal errors may be retryable
		return true
	default:
		return false
	}
}
