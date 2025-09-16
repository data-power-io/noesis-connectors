#!/bin/bash

# Proto generation script for all language SDKs
# This script generates protobuf code for Go, Python, and TypeScript

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
API_DIR="${PROJECT_ROOT}/api"

echo "Generating protobuf code for all language SDKs..."

# Check if buf is installed
if ! command -v buf &> /dev/null; then
    echo "buf is required but not installed. Please install buf: https://buf.build/docs/installation"
    exit 1
fi

# Check if protoc is installed
if ! command -v protoc &> /dev/null; then
    echo "protoc is required but not installed. Please install protoc"
    exit 1
fi

cd "${API_DIR}"

# Generate using buf
echo "Using buf to generate protobuf code..."
buf generate

echo "Protobuf generation completed successfully!"

# Verify generated files exist
GO_GEN_DIR="${PROJECT_ROOT}/sdks/go/gen"
PYTHON_GEN_DIR="${PROJECT_ROOT}/sdks/python/src/noesis_connectors/gen"
NODE_GEN_DIR="${PROJECT_ROOT}/sdks/node/src/gen"

if [ -d "$GO_GEN_DIR" ] && [ "$(ls -A $GO_GEN_DIR)" ]; then
    echo "✓ Go protobuf code generated successfully"
else
    echo "✗ Go protobuf code generation failed or no files generated"
fi

if [ -d "$PYTHON_GEN_DIR" ] && [ "$(ls -A $PYTHON_GEN_DIR)" ]; then
    echo "✓ Python protobuf code generated successfully"
else
    echo "✗ Python protobuf code generation failed or no files generated"
fi

if [ -d "$NODE_GEN_DIR" ] && [ "$(ls -A $NODE_GEN_DIR)" ]; then
    echo "✓ TypeScript protobuf code generated successfully"
else
    echo "✗ TypeScript protobuf code generation failed or no files generated"
fi

echo "Done!"