/**
 * Base gRPC server implementation for Node.js connectors.
 */

import * as grpc from '@grpc/grpc-js';
import { createLogger, Logger } from 'winston';

// TODO: Import generated protobuf types
// import { ConnectorService } from '../gen/connector/v1/connector_grpc_pb';
// import * as connector from '../gen/connector/v1/connector_pb';

/**
 * Interface that connector implementations must implement.
 */
export interface ConnectorHandler {
  /**
   * Validate the connector configuration and connectivity.
   */
  checkConnection(config: Record<string, string>): Promise<void>;

  /**
   * Return platform information and available entities.
   */
  discover(request: any): Promise<any>; // TODO: Type with generated protobuf

  /**
   * Create a new session for data extraction.
   * @returns Tuple of [sessionId, expiresAtMs]
   */
  openSession(request: any): Promise<[string, number]>;

  /**
   * Close an existing session.
   */
  closeSession(sessionId: string): Promise<void>;

  /**
   * Stream data according to the specified mode.
   */
  read(
    request: any, // TODO: Type with generated protobuf
    stream: grpc.ServerWritableStream<any, any>
  ): Promise<void>;
}

/**
 * Base gRPC server for hosting connectors.
 */
export class BaseServer {
  private server: grpc.Server;
  private logger: Logger;
  private sessions: Map<string, any> = new Map();

  constructor(
    private handler: ConnectorHandler,
    logger?: Logger
  ) {
    this.server = new grpc.Server();
    this.logger = logger || createLogger({
      level: 'info',
      format: require('winston').format.json(),
      transports: [
        new require('winston').transports.Console()
      ]
    });

    // TODO: Add service implementation once protobuf code is generated
    // this.server.addService(ConnectorService, {
    //   check: this.check.bind(this),
    //   discover: this.discover.bind(this),
    //   open: this.open.bind(this),
    //   read: this.read.bind(this),
    //   close: this.close.bind(this),
    // });
  }

  /**
   * Start the gRPC server.
   */
  async start(port: number = 8080): Promise<void> {
    return new Promise((resolve, reject) => {
      const address = `0.0.0.0:${port}`;

      this.server.bindAsync(
        address,
        grpc.ServerCredentials.createInsecure(),
        (error, port) => {
          if (error) {
            reject(error);
            return;
          }

          this.server.start();
          this.logger.info(`Connector server started on port ${port}`);
          resolve();
        }
      );
    });
  }

  /**
   * Stop the gRPC server.
   */
  async stop(): Promise<void> {
    return new Promise((resolve) => {
      this.server.tryShutdown(() => {
        this.logger.info('Connector server stopped');
        resolve();
      });
    });
  }

  // TODO: Implement gRPC service methods once protobuf code is generated

  private async check(
    call: grpc.ServerUnaryCall<any, any>,
    callback: grpc.sendUnaryData<any>
  ): Promise<void> {
    try {
      await this.handler.checkConnection(call.request.config || {});
      callback(null, { ok: true, message: 'Connection successful' });
    } catch (error) {
      callback(null, {
        ok: false,
        message: error instanceof Error ? error.message : 'Unknown error'
      });
    }
  }

  private async discover(
    call: grpc.ServerUnaryCall<any, any>,
    callback: grpc.sendUnaryData<any>
  ): Promise<void> {
    try {
      const response = await this.handler.discover(call.request);
      callback(null, response);
    } catch (error) {
      callback(error);
    }
  }

  private async open(
    call: grpc.ServerUnaryCall<any, any>,
    callback: grpc.sendUnaryData<any>
  ): Promise<void> {
    try {
      const [sessionId, expiresAt] = await this.handler.openSession(call.request);
      this.sessions.set(sessionId, { expiresAt });

      callback(null, {
        sessionId,
        expiresAt
      });
    } catch (error) {
      callback(error);
    }
  }

  private async read(
    call: grpc.ServerWritableStream<any, any>
  ): Promise<void> {
    try {
      await this.handler.read(call.request, call);
      call.end();
    } catch (error) {
      call.destroy(error instanceof Error ? error : new Error('Unknown error'));
    }
  }

  private async close(
    call: grpc.ServerUnaryCall<any, any>,
    callback: grpc.sendUnaryData<any>
  ): Promise<void> {
    try {
      await this.handler.closeSession(call.request.sessionId);
      this.sessions.delete(call.request.sessionId);

      callback(null, {});
    } catch (error) {
      callback(error);
    }
  }
}