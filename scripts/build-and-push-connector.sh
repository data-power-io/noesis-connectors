#!/bin/bash

# Build and push connector Docker images and update catalog with digests
# Usage: ./build-and-push-connector.sh <connector-name> <registry-prefix>
# Example: ./build-and-push-connector.sh postgres docker.io/datapower

set -e

# Check arguments
if [ $# -ne 2 ]; then
    echo "Usage: $0 <connector-name> <registry-prefix>"
    echo "Example: $0 postgres docker.io/datapower"
    exit 1
fi

CONNECTOR_NAME="$1"
REGISTRY_PREFIX="$2"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
CONNECTOR_DIR="$ROOT_DIR/connectors/$CONNECTOR_NAME"
CATALOG_FILE="$ROOT_DIR/catalog/index.json"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}🐳 Building and pushing connector: $CONNECTOR_NAME${NC}"
echo -e "${BLUE}📍 Registry prefix: $REGISTRY_PREFIX${NC}"

# Verify connector directory exists
if [ ! -d "$CONNECTOR_DIR" ]; then
    echo -e "${RED}❌ Connector directory not found: $CONNECTOR_DIR${NC}"
    exit 1
fi

# Verify Dockerfile exists
if [ ! -f "$CONNECTOR_DIR/Dockerfile" ]; then
    echo -e "${RED}❌ Dockerfile not found: $CONNECTOR_DIR/Dockerfile${NC}"
    exit 1
fi

# Read version from connector.yaml
VERSION="1.0.0"
if [ -f "$CONNECTOR_DIR/connector.yaml" ]; then
    VERSION=$(grep "version:" "$CONNECTOR_DIR/connector.yaml" | awk '{print $2}' | head -1)
    echo -e "${BLUE}📖 Found version in connector.yaml: $VERSION${NC}"
fi

# Build image
IMAGE_NAME="$REGISTRY_PREFIX/noesis-${CONNECTOR_NAME}-connector"
IMAGE_TAG="$IMAGE_NAME:$VERSION"

echo -e "${YELLOW}🔨 Building Docker image...${NC}"
cd "$CONNECTOR_DIR"

# Build with version tag and latest tag
docker build -t "$IMAGE_TAG" -t "$IMAGE_NAME:latest" .

if [ $? -ne 0 ]; then
    echo -e "${RED}❌ Docker build failed${NC}"
    exit 1
fi

echo -e "${GREEN}✅ Docker image built successfully${NC}"

# Push image
echo -e "${YELLOW}📤 Pushing Docker image...${NC}"

# Push version tag
docker push "$IMAGE_TAG"
if [ $? -ne 0 ]; then
    echo -e "${RED}❌ Failed to push version tag${NC}"
    exit 1
fi

# Push latest tag
docker push "$IMAGE_NAME:latest"
if [ $? -ne 0 ]; then
    echo -e "${RED}❌ Failed to push latest tag${NC}"
    exit 1
fi

echo -e "${GREEN}✅ Docker image pushed successfully${NC}"

# Get image digest
echo -e "${YELLOW}🔍 Getting image digest...${NC}"
DIGEST=$(docker inspect --format='{{index .RepoDigests 0}}' "$IMAGE_TAG" 2>/dev/null || echo "")

# Ensure the digest includes the full registry prefix
if [ ! -z "$DIGEST" ] && [[ "$DIGEST" != *"$REGISTRY_PREFIX"* ]]; then
    # Extract just the digest part and prepend the full image name
    DIGEST_HASH=$(echo "$DIGEST" | awk -F'@' '{print $2}')
    if [ ! -z "$DIGEST_HASH" ]; then
        DIGEST="$IMAGE_NAME@$DIGEST_HASH"
    fi
fi

if [ -z "$DIGEST" ]; then
    # If RepoDigests is empty, pull the image to get digest
    echo -e "${YELLOW}🔄 Pulling image to get digest...${NC}"
    docker pull "$IMAGE_TAG" >/dev/null 2>&1
    DIGEST=$(docker inspect --format='{{index .RepoDigests 0}}' "$IMAGE_TAG" 2>/dev/null || echo "")

    # Ensure the digest includes the full registry prefix
    if [ ! -z "$DIGEST" ] && [[ "$DIGEST" != *"$REGISTRY_PREFIX"* ]]; then
        # Extract just the digest part and prepend the full image name
        DIGEST_HASH=$(echo "$DIGEST" | awk -F'@' '{print $2}')
        if [ ! -z "$DIGEST_HASH" ]; then
            DIGEST="$IMAGE_NAME@$DIGEST_HASH"
        fi
    fi
fi

if [ -z "$DIGEST" ]; then
    # Alternative method using docker manifest
    echo -e "${YELLOW}🔄 Using manifest to get digest...${NC}"
    MANIFEST_DIGEST=$(docker manifest inspect "$IMAGE_TAG" --verbose 2>/dev/null | grep '"digest"' | head -1 | awk -F'"' '{print $4}' || echo "")
    if [ ! -z "$MANIFEST_DIGEST" ]; then
        DIGEST="$IMAGE_NAME@$MANIFEST_DIGEST"
    fi
fi

if [ -z "$DIGEST" ]; then
    echo -e "${RED}❌ Could not get image digest. Please check manually:${NC}"
    echo -e "${YELLOW}Run: docker inspect $IMAGE_TAG${NC}"
    echo -e "${YELLOW}Or: docker manifest inspect $IMAGE_TAG${NC}"
    exit 1
fi

echo -e "${GREEN}✅ Image digest: $DIGEST${NC}"

# Update catalog file if it exists
if [ -f "$CATALOG_FILE" ]; then
    echo -e "${YELLOW}📝 Updating catalog file...${NC}"

    # Create backup
    cp "$CATALOG_FILE" "$CATALOG_FILE.backup"

    # Update the image reference in the catalog
    # This updates the correct 'image' field (not 'docker_image')
    if command -v jq &> /dev/null; then
        # Use jq if available - update the 'image' field for the correct connector
        jq --arg connector "$CONNECTOR_NAME" --arg digest "$DIGEST" \
           '.connectors[$connector].image = $digest' \
           "$CATALOG_FILE" > "$CATALOG_FILE.tmp" && mv "$CATALOG_FILE.tmp" "$CATALOG_FILE"

        # Also update the metadata timestamp
        TIMESTAMP=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
        jq --arg timestamp "$TIMESTAMP" '.metadata.updated = $timestamp' \
           "$CATALOG_FILE" > "$CATALOG_FILE.tmp" && mv "$CATALOG_FILE.tmp" "$CATALOG_FILE"

        echo -e "${GREEN}✅ Catalog updated with digest reference${NC}"
    else
        echo -e "${YELLOW}⚠️  jq not found. Please manually update the catalog:${NC}"
        echo -e "${BLUE}Update 'image' field for '$CONNECTOR_NAME' to: $DIGEST${NC}"
    fi
else
    echo -e "${YELLOW}⚠️  Catalog file not found: $CATALOG_FILE${NC}"
    echo -e "${YELLOW}Manual step: Update your catalog with this digest reference:${NC}"
    echo -e "${BLUE}$DIGEST${NC}"
fi

echo ""
echo -e "${GREEN}🎉 Connector build and push completed!${NC}"
echo ""
echo -e "${BLUE}📋 Summary:${NC}"
echo -e "  Connector: $CONNECTOR_NAME"
echo -e "  Image: $IMAGE_TAG"
echo -e "  Digest: $DIGEST"
echo ""
echo -e "${BLUE}📝 Next steps:${NC}"
echo -e "  1. Verify the catalog file is updated with the digest reference"
echo -e "  2. Commit and push your catalog changes to git"
echo -e "  3. Add the git catalog source to your Noesis platform"
echo ""