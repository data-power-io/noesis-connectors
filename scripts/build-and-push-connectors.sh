#!/bin/bash

# Build and Push Connector Images to Local Registry
set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

error() {
    echo -e "${RED}[ERROR]${NC} $1"
    exit 1
}

# Configuration
REGISTRY_NAME="${REGISTRY_NAME:-registry.kube-system.svc.cluster.local}"
IMAGE_TAG="${IMAGE_TAG:-latest}"
CONNECTORS=("postgres")

# Check if minikube is running
check_minikube() {
    log "Checking if minikube is running..."
    if ! minikube status &> /dev/null; then
        error "Minikube is not running. Please start minikube first with 'minikube start'"
    fi
    log "Minikube is running ✓"
}

# Switch to minikube docker daemon
setup_minikube_docker() {
    log "Switching to minikube docker daemon..."
    eval $(minikube docker-env)
    log "Using minikube docker daemon ✓"
}

# Build connector image in minikube
build_connector() {
    local connector=$1
    local dockerfile="connectors/$connector/Dockerfile"
    local image_name="noesis-connector-$connector:$IMAGE_TAG"
    local build_context="connectors/$connector"

    if [ ! -f "$dockerfile" ]; then
        error "Dockerfile not found: $dockerfile"
    fi

    log "Building $connector connector image in minikube docker daemon..."
    docker build -t "$image_name" -f "$dockerfile" "$build_context"

    log "Successfully built: $image_name ✓"
}

# Main execution
main() {
    log "Building connector images in minikube docker daemon..."

    check_minikube

    setup_minikube_docker

    for connector in "${CONNECTORS[@]}"; do
        build_connector "$connector"
    done

    log "All connector images built successfully! 🎉"
    log ""
    log "Images are now available in minikube's docker daemon:"
    for connector in "${CONNECTORS[@]}"; do
        log "  noesis-connector-$connector:$IMAGE_TAG"
    done
    log ""
    log "To use these images in Kubernetes deployments, reference them as:"
    for connector in "${CONNECTORS[@]}"; do
        log "  image: noesis-connector-$connector:$IMAGE_TAG"
        log "  imagePullPolicy: IfNotPresent"
    done
}

# Handle command line arguments
case "${1:-}" in
    --help|-h)
        echo "Build connector images in minikube docker daemon"
        echo ""
        echo "Usage: $0 [OPTIONS]"
        echo ""
        echo "Options:"
        echo "  --help, -h          Show this help message"
        echo "  --list-images       List built connector images in minikube"
        echo ""
        echo "Environment variables:"
        echo "  REGISTRY_NAME       Registry name (default: registry.kube-system.svc.cluster.local)"
        echo "  IMAGE_TAG          Image tag (default: latest)"
        echo ""
        echo "Available connectors:"
        for connector in "${CONNECTORS[@]}"; do
            echo "  - $connector"
        done
        exit 0
        ;;
    --list-images)
        log "Listing built connector images in minikube docker daemon..."
        setup_minikube_docker
        docker images | grep "noesis-connector-" || echo "No connector images found"
        exit 0
        ;;
    "")
        main
        ;;
    *)
        error "Unknown option: $1. Use --help for usage information."
        ;;
esac