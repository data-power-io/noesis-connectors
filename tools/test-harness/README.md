# Connector Test Harness

A comprehensive testing framework for validating connector implementations against the Noesis connector specification.

## Features

- **Protocol Compliance**: Tests gRPC API compliance
- **Data Validation**: Validates extracted data formats and schemas
- **Performance Testing**: Measures extraction performance and resource usage
- **Error Handling**: Tests error scenarios and edge cases
- **Authentication**: Validates authentication mechanisms
- **Resumability**: Tests cursor-based resumption

## Usage

### Basic Test Run

```bash
# Test a local connector
./test-harness --connector localhost:8080 --config connector-config.yaml

# Test with specific scenarios
./test-harness --connector localhost:8080 --scenarios auth,discovery,extraction

# Generate detailed report
./test-harness --connector localhost:8080 --report-format html --output test-report.html
```

### Configuration

Create a `test-config.yaml` file:

```yaml
connector:
  name: "postgres-connector"
  version: "1.0.0"
  endpoint: "localhost:8080"

test_config:
  timeout: 30s
  max_records: 1000
  performance_threshold:
    connection_time: 5s
    first_record_time: 10s
    records_per_second: 100

scenarios:
  - name: "basic_discovery"
    enabled: true
  - name: "full_table_extraction"
    enabled: true
    config:
      entity: "test_table"
  - name: "incremental_extraction"
    enabled: true
    config:
      entity: "test_table"
      cursor_field: "updated_at"

source_config:
  host: "localhost"
  port: 5432
  database: "test_db"
  username: "test_user"
  password: "test_pass"
```

### Test Scenarios

#### Discovery Tests
- Platform information validation
- Entity discovery
- Schema validation
- Capability checks

#### Extraction Tests
- Full table scans
- Incremental extraction
- Graph traversal (if supported)
- Large dataset handling
- Empty result handling

#### Error Handling Tests
- Invalid configuration
- Network failures
- Authentication failures
- Rate limiting
- Malformed requests

#### Performance Tests
- Connection establishment time
- Time to first record
- Throughput measurement
- Memory usage
- CPU usage

## Development

### Adding New Test Scenarios

1. Create a new test file in `scenarios/`
2. Implement the `TestScenario` interface
3. Register the scenario in `main.go`

### Custom Validators

Implement the `DataValidator` interface to add custom data validation logic.

## CI Integration

The test harness can be integrated into CI pipelines:

```yaml
# GitHub Actions example
- name: Test Connector
  run: |
    ./tools/test-harness/test-harness \
      --connector ${{ env.CONNECTOR_ENDPOINT }} \
      --config .github/test-config.yaml \
      --report-format junit \
      --output test-results.xml
```