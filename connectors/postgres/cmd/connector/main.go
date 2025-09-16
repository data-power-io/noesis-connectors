package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/data-power-io/noesis-connectors/connectors/postgres/internal/config"
	"github.com/data-power-io/noesis-connectors/connectors/postgres/internal/postgres"
	"github.com/data-power-io/noesis-connectors/sdks/go/server"
	noesisv1 "github.com/data-power-io/noesis-protocol/languages/go/datapower/noesis/v1"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "health" {
		os.Exit(healthCheck())
	}

	logger, err := zap.NewProduction()
	if err != nil {
		log.Fatalf("Failed to create logger: %v", err)
	}
	defer logger.Sync()

	cfg, err := config.Load()
	if err != nil {
		logger.Fatal("Failed to load configuration", zap.Error(err))
	}

	handler, err := postgres.NewHandler(cfg, logger)
	if err != nil {
		logger.Fatal("Failed to create PostgreSQL handler", zap.Error(err))
	}
	defer handler.Close()

	baseServer := server.NewBaseServer(handler, logger)

	grpcServer := grpc.NewServer()
	noesisv1.RegisterConnectorServer(grpcServer, baseServer)

	healthServer := health.NewServer()
	healthpb.RegisterHealthServer(grpcServer, healthServer)
	healthServer.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)

	port := cfg.GetString("port", "8080")
	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		logger.Fatal("Failed to listen", zap.String("port", port), zap.Error(err))
	}

	logger.Info("PostgreSQL connector starting", zap.String("port", port))

	go func() {
		if err := grpcServer.Serve(listener); err != nil {
			logger.Fatal("Failed to serve", zap.Error(err))
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	logger.Info("Shutting down server...")
	grpcServer.GracefulStop()
	logger.Info("Server stopped")
}

func healthCheck() int {
	logger, err := zap.NewProduction()
	if err != nil {
		fmt.Printf("Failed to create logger: %v\n", err)
		return 1
	}
	defer logger.Sync()

	cfg, err := config.Load()
	if err != nil {
		logger.Error("Failed to load configuration", zap.Error(err))
		return 1
	}

	handler, err := postgres.NewHandler(cfg, logger)
	if err != nil {
		logger.Error("Failed to create handler", zap.Error(err))
		return 1
	}
	defer handler.Close()

	if err := handler.CheckConnection(context.Background(), cfg.GetConnectionConfig()); err != nil {
		logger.Error("Health check failed", zap.Error(err))
		return 1
	}

	return 0
}