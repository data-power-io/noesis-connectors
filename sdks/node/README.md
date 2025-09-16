# Noesis Connectors Node.js SDK

A TypeScript/Node.js SDK for building high-performance data connectors that integrate with the Noesis data migration platform.

## Features

- **Modern TypeScript**: Full type safety with TypeScript support
- **Async/Await**: Built with modern JavaScript async patterns
- **High Performance**: Efficient gRPC communication with streaming
- **Easy Development**: Simple APIs for building connectors
- **Testing Support**: Built-in testing utilities

## Installation

```bash
npm install @noesis/connectors
```

For development:

```bash
npm install @noesis/connectors --save-dev
```

## Quick Start

### Creating a Connector

```typescript
import { ConnectorHandler, BaseServer } from '@noesis/connectors';

class MyConnector implements ConnectorHandler {
  async checkConnection(config: Record<string, string>): Promise<void> {
    // Validate connection
  }

  async discover(request: any): Promise<any> {
    // Return platform info and entities
    return {
      platform: {
        name: 'MyDatabase',
        vendor: 'MyCompany',
        version: '1.0.0'
      },
      entities: []
    };
  }

  async openSession(request: any): Promise<[string, number]> {
    // Create session
    const sessionId = `session-${Date.now()}`;
    const expiresAt = Date.now() + 30 * 60 * 1000; // 30 minutes
    return [sessionId, expiresAt];
  }

  async closeSession(sessionId: string): Promise<void> {
    // Clean up session
  }

  async read(request: any, stream: any): Promise<void> {
    // Stream data
  }
}

async function main() {
  const handler = new MyConnector();
  const server = new BaseServer(handler);

  await server.start(8080);
  console.log('Connector server started on port 8080');
}

main().catch(console.error);
```

### Using the Client

```typescript
import { ConnectorClient } from '@noesis/connectors';

async function main() {
  const client = new ConnectorClient({
    address: 'localhost:8080'
  });

  try {
    // Check connection
    const checkResult = await client.check('tenant-1', {
      host: 'localhost',
      port: '5432'
    });

    // Discover entities
    const discovery = await client.discover({
      tenantId: 'tenant-1',
      includeSchemas: true
    });

    console.log(`Found ${discovery.entities.length} entities`);

    // Open session and read data
    const session = await client.open({
      tenantId: 'tenant-1',
      config: { host: 'localhost', port: '5432' }
    });

    for await (const message of client.read({
      sessionId: session.sessionId,
      mode: { fullTable: { entity: 'users' } }
    })) {
      console.log('Received:', message);
    }

    // Close session
    await client.close(session.sessionId);

  } finally {
    await client.disconnect();
  }
}

main().catch(console.error);
```

## Development

### Setup

```bash
# Clone and install dependencies
git clone https://github.com/data-power-io/noesis-connectors.git
cd noesis-connectors/sdks/node
npm install
```

### Build and Test

```bash
# Build TypeScript
npm run build

# Run tests
npm test

# Run tests with coverage
npm run test:coverage

# Lint code
npm run lint

# Format code
npm run format

# Type check
npm run type-check
```

### Development Workflow

```bash
# Watch mode for development
npm run build:watch

# Test watch mode
npm run test:watch
```

## API Reference

### ConnectorHandler

Interface for implementing connectors. Implement all methods to create your connector.

**Methods:**
- `checkConnection(config)`: Validate connector configuration
- `discover(request)`: Return platform information and entities
- `openSession(request)`: Create a new extraction session
- `closeSession(sessionId)`: Close an existing session
- `read(request, stream)`: Stream data according to the request

### BaseServer

gRPC server that hosts your connector implementation.

**Constructor:**
- `new BaseServer(handler, logger?)`

**Methods:**
- `start(port?)`: Start the server on the specified port
- `stop()`: Stop the server gracefully

### ConnectorClient

Client for communicating with connectors.

**Constructor:**
- `new ConnectorClient(config, logger?)`

**Methods:**
- `check(tenantId, config)`: Test connectivity
- `discover(request)`: Discover available entities
- `open(request)`: Open an extraction session
- `read(request)`: Stream data from the connector
- `close(sessionId)`: Close a session
- `disconnect()`: Close the client connection

## Configuration

### Client Configuration

```typescript
interface ConnectorClientConfig {
  address: string;           // gRPC server address
  timeout?: number;          // Request timeout in ms (default: 30000)
  maxRetries?: number;       // Max retry attempts (default: 3)
  grpcOptions?: Record<string, any>; // Additional gRPC options
}
```

## Testing

The SDK includes testing utilities:

```typescript
import { ConnectorHandler, BaseServer } from '@noesis/connectors';

describe('MyConnector', () => {
  let server: BaseServer;
  let handler: MyConnector;

  beforeEach(async () => {
    handler = new MyConnector();
    server = new BaseServer(handler);
    await server.start(0); // Random port
  });

  afterEach(async () => {
    await server.stop();
  });

  it('should check connection', async () => {
    // Test your connector
  });
});
```

## Contributing

See the main repository [CONTRIBUTING.md](../../CONTRIBUTING.md) for contribution guidelines.

## License

Apache 2.0 - see [LICENSE](../../LICENSE) for details.