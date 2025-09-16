"""Base server implementation for Python connectors."""

import asyncio
import logging
from abc import ABC, abstractmethod
from typing import Any, AsyncIterator, Dict, Optional

import grpc
import structlog

# TODO: Update these imports once protobuf code is generated
# from .gen.connector.v1 import connector_pb2_grpc as connector_grpc
# from .gen.connector.v1 import connector_pb2 as connector_pb


class ConnectorHandler(ABC):
    """Abstract base class for connector implementations."""

    @abstractmethod
    async def check_connection(
        self, config: Dict[str, str]
    ) -> None:
        """Validate the connector configuration and connectivity.

        Args:
            config: Configuration parameters for the connector

        Raises:
            Exception: If connection validation fails
        """
        pass

    @abstractmethod
    async def discover(
        self, request: Any  # connector_pb.DiscoverRequest
    ) -> Any:  # connector_pb.DiscoverResponse
        """Return platform information and available entities.

        Args:
            request: Discovery request

        Returns:
            Discovery response with platform info and entities
        """
        pass

    @abstractmethod
    async def open_session(
        self, request: Any  # connector_pb.OpenRequest
    ) -> tuple[str, int]:
        """Create a new session for data extraction.

        Args:
            request: Open session request

        Returns:
            Tuple of (session_id, expires_at_unix_ms)
        """
        pass

    @abstractmethod
    async def close_session(self, session_id: str) -> None:
        """Close an existing session.

        Args:
            session_id: ID of the session to close
        """
        pass

    @abstractmethod
    async def read(
        self,
        request: Any,  # connector_pb.ReadRequest
        stream: AsyncIterator[Any],  # AsyncIterator[connector_pb.ReadMessage]
    ) -> None:
        """Stream data according to the specified mode.

        Args:
            request: Read request
            stream: Stream to send data messages
        """
        pass


class BaseServer:
    """Base gRPC server implementation for connectors."""

    def __init__(
        self,
        handler: ConnectorHandler,
        logger: Optional[structlog.stdlib.BoundLogger] = None,
    ):
        """Initialize the base server.

        Args:
            handler: Connector implementation
            logger: Optional logger instance
        """
        self.handler = handler
        self.logger = logger or structlog.get_logger()
        self._sessions: Dict[str, Any] = {}

    async def start_server(
        self,
        port: int = 8080,
        max_workers: int = 10,
    ) -> None:
        """Start the gRPC server.

        Args:
            port: Port to listen on
            max_workers: Maximum number of worker threads
        """
        server = grpc.aio.server()

        # TODO: Add service to server once protobuf code is generated
        # connector_grpc.add_ConnectorServicer_to_server(self, server)

        listen_addr = f"[::]:{port}"
        server.add_insecure_port(listen_addr)

        self.logger.info("Starting connector server", port=port)
        await server.start()

        try:
            await server.wait_for_termination()
        except KeyboardInterrupt:
            self.logger.info("Shutting down connector server")
            await server.stop(grace=5)

    # TODO: Implement gRPC service methods once protobuf code is generated