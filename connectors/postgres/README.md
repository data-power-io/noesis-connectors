# PostgreSQL Connector

A high-performance connector for extracting data from PostgreSQL databases.

## Features

- **Full Table Scans**: Extract complete table data
- **Incremental Extraction**: Extract only changed records using timestamps or sequences
- **Schema Discovery**: Automatically discover tables, views, and relationships
- **High Performance**: Optimized queries with configurable batch sizes
- **SSL Support**: Secure connections with SSL/TLS
- **Connection Pooling**: Efficient connection management

## Supported PostgreSQL Versions

- PostgreSQL 12.x
- PostgreSQL 13.x
- PostgreSQL 14.x
- PostgreSQL 15.x
- PostgreSQL 16.x

## Configuration

### Required Parameters

| Parameter | Description | Example |
|-----------|-------------|---------|
| `host` | PostgreSQL server hostname | `localhost` |
| `port` | PostgreSQL server port | `5432` |
| `database` | Database name | `mydb` |
| `username` | Database username | `postgres` |
| `password` | Database password | `secret` |

### Optional Parameters

| Parameter | Description | Default | Example |
|-----------|-------------|---------|---------|
| `sslmode` | SSL connection mode | `prefer` | `require` |
| `schema` | Default schema to use | `public` | `app_schema` |
| `connect_timeout` | Connection timeout | `30s` | `60s` |
| `statement_timeout` | Query timeout | `300s` | `600s` |

### SSL Modes

- `disable`: No SSL
- `require`: Require SSL (default)
- `verify-ca`: Verify CA certificate
- `verify-full`: Verify CA and hostname

## Usage

### Docker

```bash
docker run -d \
  --name postgres-connector \
  -p 8080:8080 \
  -e POSTGRES_HOST=localhost \
  -e POSTGRES_PORT=5432 \
  -e POSTGRES_DATABASE=mydb \
  -e POSTGRES_USERNAME=postgres \
  -e POSTGRES_PASSWORD=secret \
  noesis/postgres-connector:latest
```

### Configuration File

```yaml
host: localhost
port: 5432
database: mydb
username: postgres
password: secret
sslmode: require
schema: public
connect_timeout: 30s
statement_timeout: 300s
```

## Extraction Modes

### Full Table Scan

Extracts all records from a table:

```json
{
  "mode": {
    "fullTable": {
      "entity": "users"
    }
  }
}
```

### Incremental Extraction

Extracts records changed since a specific point in time:

```json
{
  "mode": {
    "incremental": {
      "entity": "users",
      "cursorField": "updated_at",
      "cursor": "2023-01-01T00:00:00Z"
    }
  }
}
```

## Performance Tuning

### Batch Size

Adjust the batch size based on your needs:

```yaml
batch_size: 10000  # Default
```

### Connection Pool

Configure connection pooling:

```yaml
max_connections: 10    # Maximum connections
idle_timeout: 300s     # Idle connection timeout
```

### Query Optimization

- Use appropriate indexes on cursor fields
- Consider partitioning large tables
- Monitor query performance and adjust timeouts

## Data Types

The connector maps PostgreSQL data types to Arrow types:

| PostgreSQL Type | Arrow Type | Notes |
|----------------|------------|-------|
| `boolean` | `bool` | |
| `smallint` | `int16` | |
| `integer` | `int32` | |
| `bigint` | `int64` | |
| `real` | `float32` | |
| `double precision` | `float64` | |
| `varchar`, `text` | `string` | |
| `timestamp` | `timestamp[us]` | |
| `date` | `date32` | |
| `json`, `jsonb` | `string` | JSON as string |
| `uuid` | `string` | UUID as string |
| `bytea` | `binary` | |

## Limitations

- Complex data types (arrays, custom types) are extracted as JSON strings
- Large objects (LOBs) are not supported
- Streaming replication is not supported (use incremental extraction instead)

## Security

### Best Practices

1. **Use SSL**: Always enable SSL in production
2. **Least Privilege**: Create dedicated read-only users
3. **Network Security**: Restrict network access to the database
4. **Credential Management**: Use secure credential storage

### Read-Only User Setup

```sql
-- Create read-only user
CREATE USER connector_user WITH PASSWORD 'secure_password';

-- Grant schema usage
GRANT USAGE ON SCHEMA public TO connector_user;

-- Grant select on all tables
GRANT SELECT ON ALL TABLES IN SCHEMA public TO connector_user;

-- Grant select on future tables
ALTER DEFAULT PRIVILEGES IN SCHEMA public
  GRANT SELECT ON TABLES TO connector_user;
```

## Monitoring

The connector exposes Prometheus metrics:

- `postgres_connections_active`: Active database connections
- `postgres_queries_total`: Total queries executed
- `postgres_query_duration_seconds`: Query execution time
- `postgres_rows_extracted_total`: Total rows extracted
- `postgres_errors_total`: Total errors encountered

## Troubleshooting

### Common Issues

#### Connection Refused
```
Error: connection refused
```
- Check host and port
- Verify PostgreSQL is running
- Check firewall settings

#### Authentication Failed
```
Error: authentication failed
```
- Verify username and password
- Check pg_hba.conf configuration
- Ensure user has necessary permissions

#### SSL Connection Failed
```
Error: SSL connection failed
```
- Verify SSL configuration
- Check certificate validity
- Try different SSL modes

#### Query Timeout
```
Error: query timeout
```
- Increase `statement_timeout`
- Optimize queries with indexes
- Reduce batch size

### Debug Mode

Enable debug logging:

```yaml
log_level: debug
```

## Development

### Building

```bash
go build -o connector ./cmd/connector
```

### Testing

```bash
go test ./...
```

### Integration Tests

```bash
docker-compose up -d postgres
go test -tags=integration ./tests/...
```

## Support

For issues and questions:

- GitHub Issues: https://github.com/data-power-io/noesis-connectors/issues
- Documentation: https://docs.noesis.dev/connectors/postgres
- Community: https://community.noesis.dev