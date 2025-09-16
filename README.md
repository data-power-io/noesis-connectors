# Noesis Connectors

An open-source, language-agnostic framework for building data connectors. This repository provides APIs, SDKs, tools, and reference implementations for creating high-performance data extraction connectors that integrate with the Noesis data migration platform.

## 🚀 Features

- **Language Agnostic**: SDKs available in Go, Python, and Node.js
- **High Performance**: Efficient data streaming using Apache Arrow format
- **Multiple Extraction Modes**: Full Table, Change Data Capture, and Subgraph traversal
- **Resumable Operations**: Built-in cursor management for fault tolerance
- **Rich Discovery**: Comprehensive metadata and schema discovery
- **Production Ready**: Battle-tested with enterprise systems

## 🏗️ Architecture

The repository is organized into several key areas:

- **api/**: Language-agnostic protobuf API definitions
- **sdks/**: Language-specific SDKs (Go, Python, Node.js)
- **libs/**: Shared libraries for logging, metrics, and data format helpers
- **tools/**: Development tools including scaffolding and test harness
- **connectors/**: Individual connector implementations
- **catalog/**: Auto-generated connector catalog and schemas
- **docs/**: Comprehensive documentation and guides

## Quick Start

### Creating a Connector Server

```go
package main

import (
    "context"
    "log"
    "net"
    "time"

    "github.com/data-power-io/noesis-connectors/server"
    connectorv1 "github.com/data-power-io/noesis-connectors/pkg/v1"
    "go.uber.org/zap"
    "google.golang.org/grpc"
)

// MyConnector implements the ConnectorHandler interface
type MyConnector struct {
    logger *zap.Logger
}

func (c *MyConnector) CheckConnection(ctx context.Context, config map[string]string) error {
    // Implement connection validation logic
    return nil
}

func (c *MyConnector) Discover(ctx context.Context, req *connectorv1.DiscoverRequest) (*connectorv1.DiscoverResponse, error) {
    // Implement discovery logic
    return &connectorv1.DiscoverResponse{
        Platform: &connectorv1.PlatformInfo{
            Name:    "MyDatabase",
            Vendor:  "MyCompany",
            Version: "1.0.0",
        },
        Entities: []*connectorv1.EntityDescriptor{
            // Add entity descriptors
        },
    }, nil
}

func (c *MyConnector) OpenSession(ctx context.Context, req *connectorv1.OpenRequest) (string, time.Time, error) {
    // Create session and return ID and expiration
    sessionID := "session-" + time.Now().Format("20060102150405")
    expiresAt := time.Now().Add(30 * time.Minute)
    return sessionID, expiresAt, nil
}

func (c *MyConnector) CloseSession(ctx context.Context, sessionID string) error {
    // Clean up session resources
    return nil
}

func (c *MyConnector) Read(ctx context.Context, req *connectorv1.ReadRequest, stream server.ReadStream) error {
    // Implement data extraction logic
    // Use the stream parameter to send data back to the client
    return nil
}

func main() {
    logger, _ := zap.NewDevelopment()

    // Create connector handler
    handler := &MyConnector{logger: logger}

    // Create base server
    baseServer := server.NewBaseServer(handler, logger)

    // Create gRPC server
    grpcServer := grpc.NewServer()
    connectorv1.RegisterConnectorServer(grpcServer, baseServer)

    // Start listening
    lis, err := net.Listen("tcp", ":8080")
    if err != nil {
        log.Fatalf("Failed to listen: %v", err)
    }

    log.Println("Connector server starting on :8080")
    if err := grpcServer.Serve(lis); err != nil {
        log.Fatalf("Failed to serve: %v", err)
    }
}
```

### Using the gRPC Client

```go
package main

import (
    "context"
    "log"

    "github.com/data-power-io/noesis-connectors/client"
    connectorv1 "github.com/data-power-io/noesis-connectors/pkg/v1"
    "go.uber.org/zap"
)

func main() {
    logger, _ := zap.NewDevelopment()

    // Create client configuration
    config := client.DefaultClientConfig("localhost:8080")

    // Create gRPC client
    grpcClient, err := client.NewGRPCClient(config, logger)
    if err != nil {
        log.Fatalf("Failed to create client: %v", err)
    }
    defer grpcClient.CloseConnection()

    ctx := context.Background()

    // Test connection
    checkResp, err := grpcClient.Check(ctx, "tenant-1", map[string]string{
        "host": "localhost",
        "port": "5432",
    })
    if err != nil {
        log.Fatalf("Check failed: %v", err)
    }

    if !checkResp.Ok {
        log.Fatalf("Connection check failed: %s", checkResp.Message)
    }

    // Discover entities
    discoverResp, err := grpcClient.Discover(ctx, &connectorv1.DiscoverRequest{
        TenantId:       "tenant-1",
        IncludeSchemas: true,
    })
    if err != nil {
        log.Fatalf("Discovery failed: %v", err)
    }

    log.Printf("Found %d entities", len(discoverResp.Entities))

    // Open session
    openResp, err := grpcClient.Open(ctx, &connectorv1.OpenRequest{
        TenantId: "tenant-1",
        Config: map[string]string{
            "host": "localhost",
            "port": "5432",
        },
    })
    if err != nil {
        log.Fatalf("Open session failed: %v", err)
    }

    // Start reading data
    readReq := &connectorv1.ReadRequest{
        SessionId: openResp.SessionId,
        Mode: &connectorv1.ReadRequest_FullTable{
            FullTable: &connectorv1.FullTableScan{
                Entity: "users",
            },
        },
    }

    // Handle streaming messages
    handler := client.MessageHandlerFunc(func(msg *connectorv1.ReadMessage) error {
        switch m := msg.Msg.(type) {
        case *connectorv1.ReadMessage_Record:
            log.Printf("Received record: %s", m.Record.Key)
        case *connectorv1.ReadMessage_State:
            log.Printf("Received state checkpoint")
        case *connectorv1.ReadMessage_Schema:
            log.Printf("Received schema for entity: %s", m.Schema.Entity)
        }
        return nil
    })

    err = grpcClient.Read(ctx, readReq, handler)
    if err != nil {
        log.Fatalf("Read failed: %v", err)
    }

    // Close session
    err = grpcClient.Close(ctx, openResp.SessionId)
    if err != nil {
        log.Printf("Close session failed: %v", err)
    }
}
```

## Schema Management

The SDK provides utilities for working with Apache Arrow schemas:

```go
import "github.com/data-power-io/noesis-connectors/schema"

// Create schema manager
schemaManager := schema.NewArrowSchemaManager()

// Build schema from field definitions
fields := map[string]string{
    "id":         "int64",
    "name":       "string",
    "email":      "string",
    "created_at": "timestamp",
    "active":     "boolean",
}

arrowSchema := schemaManager.BuildSchemaFromFields(fields)

// Convert to proto descriptor
descriptor, err := schemaManager.SchemaToDescriptor(arrowSchema, "users_v1")
if err != nil {
    log.Fatalf("Failed to create descriptor: %v", err)
}
```

## Cursor Management

Handle resumable operations with cursor management:

```go
import "github.com/data-power-io/noesis-connectors/cursor"

// Create cursor manager
cursorManager := cursor.NewManager()

// Create offset-based cursor
offsetCursor, err := cursorManager.CreateOffsetCursor(1000, 100)
if err != nil {
    log.Fatalf("Failed to create cursor: %v", err)
}

// Parse cursor in handler
parsedCursor, err := cursorManager.ParseOffsetCursor(offsetCursor)
if err != nil {
    log.Fatalf("Failed to parse cursor: %v", err)
}

log.Printf("Resume from offset: %d", parsedCursor.Offset)
```

## Data Streaming

Stream data efficiently using the streaming utilities:

```go
import (
    "github.com/data-power-io/noesis-connectors/streaming"
    "github.com/apache/arrow/go/v18/arrow"
)

// In your Read method implementation
func (c *MyConnector) Read(ctx context.Context, req *connectorv1.ReadRequest, stream server.ReadStream) error {
    // Create record streamer
    schema := buildYourArrowSchema()
    streamer := streaming.NewRecordStreamer(stream, c.logger, schema, "entity_v1")

    // Send schema first
    err := streamer.SendSchema("users")
    if err != nil {
        return err
    }

    // Stream records
    for _, record := range getYourData() {
        payload, _ := json.Marshal(record)
        err := streamer.SendRecord(
            "users",           // entity
            record.ID,         // key
            payload,           // payload
            connectorv1.Op_UPSERT, // operation
            "",                // group ID
        )
        if err != nil {
            return err
        }
    }

    // Send checkpoint
    cursor, _ := createCheckpointCursor()
    err = streamer.SendState(cursor, time.Now().UnixMilli(), "")
    if err != nil {
        return err
    }

    return nil
}
```

## Best Practices

### Error Handling
- Always return meaningful errors from your connector methods
- Use structured logging for debugging
- Implement proper cleanup in CloseSession

### Performance
- Use batching when streaming large datasets
- Implement proper cursor checkpointing for resumability
- Consider memory usage when building Arrow records

### Schema Design
- Use appropriate Arrow data types for your data
- Provide meaningful entity names and descriptions
- Include proper primary key and unique key information

### Session Management
- Set reasonable session expiration times
- Clean up resources properly in CloseSession
- Handle session expiration gracefully

## Testing

Create unit tests for your connector:

```go
func TestMyConnector(t *testing.T) {
    logger := zap.NewNop()
    connector := &MyConnector{logger: logger}

    // Test connection
    err := connector.CheckConnection(context.Background(), map[string]string{
        "host": "testhost",
    })
    assert.NoError(t, err)

    // Test discovery
    resp, err := connector.Discover(context.Background(), &connectorv1.DiscoverRequest{
        TenantId: "test-tenant",
    })
    assert.NoError(t, err)
    assert.NotNil(t, resp.Platform)
    assert.Greater(t, len(resp.Entities), 0)
}
```

## Building and Development

### Prerequisites

- Go 1.24 or later
- Protocol Buffers compiler (`protoc`)
- Make

### Setup

1. Clone the repository:
```bash
git clone https://github.com/data-power-io/noesis-connectors.git
cd noesis-connectors
```

2. Install development tools:
```bash
make install-tools
```

3. Generate protobuf files and build:
```bash
make all
```

4. Run tests:
```bash
make test
```

### Available Make Targets

- `make all` - Generate proto and build (default)
- `make proto` - Generate protobuf code
- `make build` - Build all packages
- `make test` - Run tests
- `make lint` - Run linter
- `make fmt` - Format code
- `make clean` - Clean generated files
- `make check` - Run all quality checks

### Building Your Connector

1. Build your connector as a Docker image
2. Include health checks and readiness probes
3. Configure resource limits appropriately
4. Use multi-stage builds for smaller images