# Contributing to Noesis Connectors

Thank you for your interest in contributing to the Noesis Connectors SDK! This document provides guidelines for contributing to the project.

## Table of Contents

- [Code of Conduct](#code-of-conduct)
- [Getting Started](#getting-started)
- [Development Process](#development-process)
- [Submitting Changes](#submitting-changes)
- [Coding Standards](#coding-standards)
- [Testing](#testing)
- [Documentation](#documentation)

## Code of Conduct

This project adheres to a code of conduct that we expect all contributors to follow. Please be respectful and constructive in all interactions.

## Getting Started

### Prerequisites

**Core Requirements:**
- Git
- Protocol Buffers compiler (`protoc`)
- Make (or equivalent build tools)

**Language-Specific Requirements:**

**Go Development:**
- Go 1.24 or later
- golangci-lint for linting

**Python Development:**
- Python 3.9 or later
- Poetry or pip for dependency management
- Black for code formatting
- Ruff for linting

**Node.js Development:**
- Node.js 18 or later
- npm or yarn for dependency management
- ESLint and Prettier for code quality

### Development Setup

1. Fork the repository on GitHub
2. Clone your fork locally:
   ```bash
   git clone https://github.com/YOUR-USERNAME/noesis-connectors.git
   cd noesis-connectors
   ```

3. Install development tools:
   ```bash
   make install-tools
   ```

4. Build and test:
   ```bash
   make all
   make test
   ```

## Development Process

### Branch Naming

Use descriptive branch names:
- `feature/add-mysql-connector` - for new features
- `fix/connection-timeout-bug` - for bug fixes
- `docs/update-readme` - for documentation updates
- `refactor/schema-manager` - for refactoring

### Commit Messages

Follow conventional commit format:
```
type(scope): description

[optional body]

[optional footer]
```

Types:
- `feat`: new feature
- `fix`: bug fix
- `docs`: documentation changes
- `style`: formatting, missing semicolons, etc.
- `refactor`: code changes that neither fix bugs nor add features
- `test`: adding or updating tests
- `chore`: maintenance tasks

Examples:
```
feat(client): add connection pooling support
fix(server): handle graceful shutdown properly
docs(readme): update installation instructions
```

## Submitting Changes

### Pull Request Process

1. Create a feature branch from `main`
2. Make your changes
3. Add or update tests as needed
4. Ensure all tests pass: `make test`
5. Run quality checks: `make check`
6. Update documentation if needed
7. Submit a pull request

### Pull Request Guidelines

- Provide a clear description of the changes
- Reference any related issues
- Include test coverage for new functionality
- Ensure CI checks pass
- Request review from maintainers

## Coding Standards

### Go Style Guide

- Follow standard Go formatting: `make fmt`
- Use `go vet`: `make vet`
- Follow effective Go practices
- Use meaningful variable and function names
- Write clear, concise comments for public APIs

### Code Organization

- Keep packages focused and cohesive
- Use interfaces to define contracts
- Prefer composition over inheritance
- Handle errors explicitly
- Use context for cancellation and timeouts

### Error Handling

```go
// Good
result, err := someOperation()
if err != nil {
    return fmt.Errorf("operation failed: %w", err)
}

// Avoid
result, _ := someOperation() // ignoring errors
```

### Logging

Use structured logging with zap:
```go
logger.Info("operation completed",
    zap.String("entity", entityName),
    zap.Int("recordCount", count),
    zap.Duration("duration", elapsed))
```

## Testing

### Test Requirements

- Unit tests for all new functionality
- Integration tests for complex interactions
- Benchmark tests for performance-critical code
- Examples in documentation should be tested

### Writing Tests

```go
func TestConnectorHandler_CheckConnection(t *testing.T) {
    logger := zap.NewNop()
    handler := &MyConnector{logger: logger}

    config := map[string]string{
        "host": "localhost",
        "port": "5432",
    }

    err := handler.CheckConnection(context.Background(), config)
    assert.NoError(t, err)
}
```

### Running Tests

```bash
# Run all tests
make test

# Run tests with coverage
make test-coverage

# Run specific package tests
go test ./client/...

# Run tests with verbose output
go test -v ./...
```

## Documentation

### Code Documentation

- Document all public APIs with godoc comments
- Include examples in documentation
- Keep comments up to date with code changes

### README Updates

- Update README.md for significant changes
- Include usage examples
- Document new configuration options

### Godoc Comments

```go
// ArrowSchemaManager provides utilities for working with Apache Arrow schemas.
// It supports converting between different schema formats and building schemas
// from field definitions.
type ArrowSchemaManager struct{}

// BuildSchemaFromFields creates an Arrow schema from a map of field names to types.
// Supported types include: string, int64, float64, boolean, timestamp.
//
// Example:
//   fields := map[string]string{
//       "id": "int64",
//       "name": "string",
//   }
//   schema := manager.BuildSchemaFromFields(fields)
func (m *ArrowSchemaManager) BuildSchemaFromFields(fields map[string]string) *arrow.Schema {
    // implementation
}
```

## Protocol Buffer Changes

When modifying protobuf definitions:

1. Update the `.proto` file in `proto/v1/`
2. Regenerate Go code: `make proto`
3. Update any affected code
4. Test thoroughly to ensure compatibility

## Release Process

Releases are handled by maintainers:

1. Update version numbers
2. Update CHANGELOG.md
3. Create release tag
4. Publish release notes

## Questions?

- Open an issue for bugs or feature requests
- Start a discussion for questions or ideas
- Check existing issues and discussions first

Thank you for contributing to Noesis Connectors!