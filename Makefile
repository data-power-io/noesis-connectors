.PHONY: all proto build test lint clean fmt vet help install-tools
.PHONY: build-go build-python build-node test-go test-python test-node
.PHONY: lint-go lint-python lint-node docs catalog

# Variables
API_DIR = api
GO_SDK_DIR = sdks/go
PYTHON_SDK_DIR = sdks/python
NODE_SDK_DIR = sdks/node
DOCS_DIR = docs
CATALOG_DIR = catalog

# Default target
all: proto build test

# Generate protobuf code for all languages
proto:
	@echo "Generating protobuf code for all languages..."
	@cd $(API_DIR) && buf generate
	@echo "Protobuf generation complete"

# Build all SDKs
build: build-go build-python build-node

# Build Go SDK
build-go: proto
	@echo "Building Go SDK..."
	@cd $(GO_SDK_DIR) && go build ./...
	@echo "Go SDK build complete"

# Build Python SDK
build-python: proto
	@echo "Building Python SDK..."
	@cd $(PYTHON_SDK_DIR) && python -m pip install -e .
	@echo "Python SDK build complete"

# Build Node.js SDK
build-node: proto
	@echo "Building Node.js SDK..."
	@cd $(NODE_SDK_DIR) && npm install && npm run build
	@echo "Node.js SDK build complete"

# Test all SDKs
test: test-go test-python test-node

# Test Go SDK
test-go:
	@echo "Running Go tests..."
	@cd $(GO_SDK_DIR) && go test -v ./...

# Test Python SDK
test-python:
	@echo "Running Python tests..."
	@cd $(PYTHON_SDK_DIR) && python -m pytest

# Test Node.js SDK
test-node:
	@echo "Running Node.js tests..."
	@cd $(NODE_SDK_DIR) && npm test

# Lint all code
lint: lint-go lint-python lint-node

# Lint Go code
lint-go:
	@echo "Linting Go code..."
	@cd $(GO_SDK_DIR) && golangci-lint run

# Lint Python code
lint-python:
	@echo "Linting Python code..."
	@cd $(PYTHON_SDK_DIR) && black --check . && ruff check .

# Lint Node.js code
lint-node:
	@echo "Linting Node.js code..."
	@cd $(NODE_SDK_DIR) && npm run lint

# Build documentation
docs:
	@echo "Building documentation..."
	@cd $(DOCS_DIR) && mkdocs build

# Generate connector catalog
catalog:
	@echo "Generating connector catalog..."
	@tools/catalog-gen/generate-catalog.sh

# Format code
fmt:
	@echo "Formatting code..."
	@go fmt $(GO_PACKAGES)

# Vet code
vet:
	@echo "Running go vet..."
	@go vet $(GO_PACKAGES)

# Clean generated files
clean:
	@echo "Cleaning generated files..."
	@rm -rf $(PKG_DIR)/v1/*.pb.go
	@echo "Clean complete"

# Install development tools
install-tools:
	@echo "Installing development tools..."
	@go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
	@go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	@go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
	@echo "Tools installed"

# Tidy dependencies
tidy:
	@echo "Tidying dependencies..."
	@go mod tidy

# Check if everything is ready for release
check: fmt vet lint test
	@echo "All checks passed!"

# Help
help:
	@echo "Available targets:"
	@echo "  all              - Generate proto and build (default)"
	@echo "  proto            - Generate protobuf code"
	@echo "  build            - Build all packages"
	@echo "  test             - Run tests"
	@echo "  test-coverage    - Run tests with coverage"
	@echo "  lint             - Run linter"
	@echo "  fmt              - Format code"
	@echo "  vet              - Run go vet"
	@echo "  clean            - Clean generated files"
	@echo "  install-tools    - Install development tools"
	@echo "  tidy             - Tidy dependencies"
	@echo "  check            - Run all quality checks"
	@echo "  help             - Show this help"