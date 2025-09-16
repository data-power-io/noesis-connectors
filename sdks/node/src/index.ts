/**
 * Noesis Connectors Node.js SDK
 *
 * A TypeScript/Node.js SDK for building high-performance data connectors
 * that integrate with the Noesis data migration platform.
 */

export { ConnectorClient } from './client/grpc-client';
export { BaseServer, ConnectorHandler } from './server/base-server';
export { CursorManager } from './cursor/manager';
export { SchemaManager } from './schema/arrow-schema';
export { RecordStreamer } from './streaming/record-streamer';

// Re-export types from generated protobuf code
// TODO: Add these exports once protobuf code is generated
// export * from './gen/connector/v1/connector_pb';