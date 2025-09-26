package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
)

type Client struct {
	pool   *pgxpool.Pool
	logger *zap.Logger
	config map[string]string
}

func NewClient(config map[string]string, logger *zap.Logger) (*Client, error) {
	dsn := buildConnectionString(config)

	poolConfig, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to parse connection config: %w", err)
	}

	poolConfig.MaxConns = 10
	poolConfig.MinConns = 1
	poolConfig.MaxConnLifetime = time.Hour
	poolConfig.MaxConnIdleTime = time.Minute * 30

	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection pool: %w", err)
	}

	client := &Client{
		pool:   pool,
		logger: logger,
		config: config,
	}

	return client, nil
}

func (c *Client) Close() {
	if c.pool != nil {
		c.pool.Close()
	}
}

func (c *Client) Ping(ctx context.Context) error {
	return c.pool.Ping(ctx)
}

func (c *Client) QueryRow(ctx context.Context, sql string, args ...interface{}) pgx.Row {
	return c.pool.QueryRow(ctx, sql, args...)
}

func (c *Client) Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error) {
	return c.pool.Query(ctx, sql, args...)
}

func (c *Client) GetSchemas(ctx context.Context) ([]string, error) {
	query := `
		SELECT schema_name
		FROM information_schema.schemata
		WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
		ORDER BY schema_name
	`

	rows, err := c.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query schemas: %w", err)
	}
	defer rows.Close()

	var schemas []string
	for rows.Next() {
		var schema string
		if err := rows.Scan(&schema); err != nil {
			return nil, fmt.Errorf("failed to scan schema: %w", err)
		}
		schemas = append(schemas, schema)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating schemas: %w", err)
	}

	return schemas, nil
}

func (c *Client) GetTables(ctx context.Context, schema string) ([]TableInfo, error) {
	query := `
		SELECT
			t.table_name,
			t.table_type,
			COALESCE(obj_description(pc.oid, 'pg_class'), '') as table_comment
		FROM information_schema.tables t
		LEFT JOIN pg_catalog.pg_namespace pn ON pn.nspname = t.table_schema
		LEFT JOIN pg_catalog.pg_class pc ON pc.relname = t.table_name AND pc.relnamespace = pn.oid
		WHERE t.table_schema = $1
		AND t.table_type IN ('BASE TABLE', 'VIEW')
		ORDER BY t.table_name
	`

	rows, err := c.Query(ctx, query, schema)
	if err != nil {
		return nil, fmt.Errorf("failed to query tables: %w", err)
	}
	defer rows.Close()

	var tables []TableInfo
	for rows.Next() {
		var table TableInfo
		if err := rows.Scan(&table.Name, &table.Type, &table.Comment); err != nil {
			return nil, fmt.Errorf("failed to scan table: %w", err)
		}
		table.Schema = schema
		tables = append(tables, table)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating tables: %w", err)
	}

	return tables, nil
}

func (c *Client) GetColumns(ctx context.Context, schema, table string) ([]ColumnInfo, error) {
	query := `
		SELECT
			column_name,
			data_type,
			is_nullable,
			column_default,
			ordinal_position,
			COALESCE(character_maximum_length, 0) as max_length,
			COALESCE(numeric_precision, 0) as precision,
			COALESCE(numeric_scale, 0) as scale
		FROM information_schema.columns
		WHERE table_schema = $1 AND table_name = $2
		ORDER BY ordinal_position
	`

	rows, err := c.Query(ctx, query, schema, table)
	if err != nil {
		return nil, fmt.Errorf("failed to query columns: %w", err)
	}
	defer rows.Close()

	var columns []ColumnInfo
	for rows.Next() {
		var col ColumnInfo
		if err := rows.Scan(
			&col.Name,
			&col.DataType,
			&col.IsNullable,
			&col.DefaultValue,
			&col.Position,
			&col.MaxLength,
			&col.Precision,
			&col.Scale,
		); err != nil {
			return nil, fmt.Errorf("failed to scan column: %w", err)
		}
		columns = append(columns, col)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating columns: %w", err)
	}

	return columns, nil
}

func buildConnectionString(config map[string]string) string {
	host := config["host"]
	port := config["port"]
	database := config["database"]
	username := config["username"]
	password := config["password"]
	sslmode := config["sslmode"]

	dsn := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=%s",
		username, password, host, port, database, sslmode)

	if connectTimeout := config["connect_timeout"]; connectTimeout != "" {
		// Convert duration string to seconds for PostgreSQL
		if duration, err := time.ParseDuration(connectTimeout); err == nil {
			seconds := int(duration.Seconds())
			dsn += fmt.Sprintf("&connect_timeout=%d", seconds)
		}
	}

	if statementTimeout := config["statement_timeout"]; statementTimeout != "" {
		// Convert duration string to seconds for PostgreSQL
		if duration, err := time.ParseDuration(statementTimeout); err == nil {
			seconds := int(duration.Seconds())
			dsn += fmt.Sprintf("&statement_timeout=%d", seconds)
		}
	}

	return dsn
}

type TableInfo struct {
	Schema  string
	Name    string
	Type    string
	Comment string
}

type ColumnInfo struct {
	Name         string
	DataType     string
	IsNullable   string
	DefaultValue *string
	Position     int
	MaxLength    int
	Precision    int
	Scale        int
}
