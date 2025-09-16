# Noesis Connectors Python SDK

A Python SDK for building high-performance data connectors that integrate with the Noesis data migration platform.

## Features

- **Async/Await Support**: Built with modern Python async patterns
- **Type Safety**: Full type annotations with mypy support
- **High Performance**: Efficient gRPC communication with streaming
- **Easy Development**: Simple APIs for building connectors
- **Testing Support**: Built-in testing utilities

## Installation

```bash
pip install noesis-connectors
```

For development:

```bash
pip install noesis-connectors[dev]
```

## Quick Start

### Creating a Connector

```python
import asyncio
from typing import Dict, Any

from noesis_connectors import ConnectorHandler, BaseServer


class MyConnector(ConnectorHandler):
    async def check_connection(self, config: Dict[str, str]) -> None:
        # Validate connection
        pass

    async def discover(self, request: Any) -> Any:
        # Return platform info and entities
        pass

    async def open_session(self, request: Any) -> tuple[str, int]:
        # Create session
        session_id = "session-123"
        expires_at = int(time.time() * 1000) + 30 * 60 * 1000  # 30 min
        return session_id, expires_at

    async def close_session(self, session_id: str) -> None:
        # Clean up session
        pass

    async def read(self, request: Any, stream) -> None:
        # Stream data
        pass


async def main():
    handler = MyConnector()
    server = BaseServer(handler)
    await server.start_server(port=8080)


if __name__ == "__main__":
    asyncio.run(main())
```

### Using the Client

```python
import asyncio
from noesis_connectors import ConnectorClient


async def main():
    async with ConnectorClient("localhost:8080") as client:
        # Check connection
        result = await client.check("tenant-1", {"host": "localhost"})

        # Discover entities
        discovery = await client.discover(request)

        # Open session and read data
        session = await client.open(request)
        async for message in client.read(read_request):
            print(f"Received: {message}")


if __name__ == "__main__":
    asyncio.run(main())
```

## Development

### Setup

```bash
# Clone and install in development mode
git clone https://github.com/data-power-io/noesis-connectors.git
cd noesis-connectors/sdks/python
pip install -e ".[dev]"
```

### Code Quality

```bash
# Format code
black .

# Lint code
ruff check .

# Type check
mypy .

# Run tests
pytest
```

## API Reference

### ConnectorHandler

Base class for implementing connectors. Override the abstract methods to implement your connector logic.

### BaseServer

gRPC server that hosts your connector implementation.

### ConnectorClient

Async client for communicating with connectors.

## Testing

The SDK includes testing utilities to help you test your connectors:

```python
import pytest
from noesis_connectors.testing import MockConnectorServer


@pytest.mark.asyncio
async def test_my_connector():
    handler = MyConnector()

    async with MockConnectorServer(handler) as server:
        async with ConnectorClient(server.address) as client:
            result = await client.check("test-tenant", {})
            assert result.ok
```

## Contributing

See the main repository [CONTRIBUTING.md](../../CONTRIBUTING.md) for contribution guidelines.

## License

Apache 2.0 - see [LICENSE](../../LICENSE) for details.