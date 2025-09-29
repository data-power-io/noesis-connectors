package postgres

import (
	"context"
	"fmt"
	"strings"
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

func (c *Client) GetConstraints(ctx context.Context, schema, table string) ([]ConstraintInfo, error) {
	query := `
		SELECT
			tc.constraint_name,
			tc.constraint_type,
			array_to_string(array_agg(kcu.column_name ORDER BY kcu.ordinal_position), ',') as columns,
			ccu.table_name as referenced_table,
			array_to_string(array_agg(ccu.column_name ORDER BY kcu.ordinal_position), ',') as referenced_columns,
			rc.delete_rule as on_delete,
			rc.update_rule as on_update,
			cc.check_clause as check_expression,
			obj_description(pgc.oid, 'pg_constraint') as documentation
		FROM information_schema.table_constraints tc
		LEFT JOIN information_schema.key_column_usage kcu
			ON tc.constraint_name = kcu.constraint_name
			AND tc.table_schema = kcu.table_schema
			AND tc.table_name = kcu.table_name
		LEFT JOIN information_schema.constraint_column_usage ccu
			ON tc.constraint_name = ccu.constraint_name
		LEFT JOIN information_schema.referential_constraints rc
			ON tc.constraint_name = rc.constraint_name
		LEFT JOIN information_schema.check_constraints cc
			ON tc.constraint_name = cc.constraint_name
		LEFT JOIN pg_catalog.pg_namespace pgn ON pgn.nspname = tc.table_schema
		LEFT JOIN pg_catalog.pg_class pgcl ON pgcl.relname = tc.table_name AND pgcl.relnamespace = pgn.oid
		LEFT JOIN pg_catalog.pg_constraint pgc ON pgc.conname = tc.constraint_name AND pgc.conrelid = pgcl.oid
		WHERE tc.table_schema = $1 AND tc.table_name = $2
		GROUP BY tc.constraint_name, tc.constraint_type, ccu.table_name, rc.delete_rule, rc.update_rule, cc.check_clause, pgc.oid
		ORDER BY tc.constraint_name
	`

	rows, err := c.Query(ctx, query, schema, table)
	if err != nil {
		return nil, fmt.Errorf("failed to query constraints: %w", err)
	}
	defer rows.Close()

	var constraints []ConstraintInfo
	for rows.Next() {
		var constraint ConstraintInfo
		var refTable, onDelete, onUpdate, checkExpr, docs interface{}
		var columnsStr, refColumnsStr *string

		if err := rows.Scan(
			&constraint.Name,
			&constraint.Type,
			&columnsStr,
			&refTable,
			&refColumnsStr,
			&onDelete,
			&onUpdate,
			&checkExpr,
			&docs,
		); err != nil {
			return nil, fmt.Errorf("failed to scan constraint: %w", err)
		}

		// Parse comma-separated column strings into slices
		if columnsStr != nil && *columnsStr != "" {
			// Filter out NULL values that might appear in aggregated results
			columnParts := strings.Split(*columnsStr, ",")
			for _, col := range columnParts {
				col = strings.TrimSpace(col)
				if col != "" && col != "NULL" {
					constraint.Columns = append(constraint.Columns, col)
				}
			}
		}

		if refColumnsStr != nil && *refColumnsStr != "" {
			// Filter out NULL values that might appear in aggregated results
			refColumnParts := strings.Split(*refColumnsStr, ",")
			for _, col := range refColumnParts {
				col = strings.TrimSpace(col)
				if col != "" && col != "NULL" {
					constraint.ReferencedColumns = append(constraint.ReferencedColumns, col)
				}
			}
		}

		// Handle nullable fields
		if refTable != nil {
			if refStr, ok := refTable.(string); ok {
				constraint.ReferencedTable = &refStr
			}
		}
		if onDelete != nil {
			if delStr, ok := onDelete.(string); ok {
				constraint.OnDelete = &delStr
			}
		}
		if onUpdate != nil {
			if updStr, ok := onUpdate.(string); ok {
				constraint.OnUpdate = &updStr
			}
		}
		if checkExpr != nil {
			if checkStr, ok := checkExpr.(string); ok {
				constraint.CheckExpression = &checkStr
			}
		}
		if docs != nil {
			if docStr, ok := docs.(string); ok {
				constraint.Documentation = &docStr
			}
		}

		constraints = append(constraints, constraint)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating constraints: %w", err)
	}

	return constraints, nil
}

func (c *Client) GetIndexes(ctx context.Context, schema, table string) ([]IndexInfo, error) {
	query := `
		SELECT
			i.relname as index_name,
			array_to_string(array_agg(a.attname ORDER BY a.attnum), ',') as columns,
			idx.indisunique as is_unique,
			am.amname as index_type,
			pg_get_expr(idx.indpred, idx.indrelid) as condition_expr,
			obj_description(i.oid, 'pg_class') as documentation
		FROM pg_catalog.pg_index idx
		JOIN pg_catalog.pg_class i ON i.oid = idx.indexrelid
		JOIN pg_catalog.pg_class t ON t.oid = idx.indrelid
		JOIN pg_catalog.pg_namespace n ON n.oid = t.relnamespace
		JOIN pg_catalog.pg_am am ON am.oid = i.relam
		JOIN pg_catalog.pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(idx.indkey)
		WHERE n.nspname = $1
		AND t.relname = $2
		AND NOT idx.indisprimary  -- Exclude primary key indexes (handled as constraints)
		GROUP BY i.relname, idx.indisunique, am.amname, idx.indpred, idx.indrelid, i.oid
		ORDER BY i.relname
	`

	rows, err := c.Query(ctx, query, schema, table)
	if err != nil {
		return nil, fmt.Errorf("failed to query indexes: %w", err)
	}
	defer rows.Close()

	var indexes []IndexInfo
	for rows.Next() {
		var index IndexInfo
		var columnsStr *string
		var condExpr, docs interface{}

		if err := rows.Scan(
			&index.Name,
			&columnsStr,
			&index.IsUnique,
			&index.Type,
			&condExpr,
			&docs,
		); err != nil {
			return nil, fmt.Errorf("failed to scan index: %w", err)
		}

		// Parse comma-separated column string into slice
		if columnsStr != nil && *columnsStr != "" {
			columnParts := strings.Split(*columnsStr, ",")
			for _, col := range columnParts {
				col = strings.TrimSpace(col)
				if col != "" {
					index.Columns = append(index.Columns, col)
				}
			}
		}

		// Handle nullable fields
		if condExpr != nil {
			if condStr, ok := condExpr.(string); ok {
				index.ConditionExpr = &condStr
			}
		}
		if docs != nil {
			if docStr, ok := docs.(string); ok {
				index.Documentation = &docStr
			}
		}

		indexes = append(indexes, index)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating indexes: %w", err)
	}

	return indexes, nil
}

func (c *Client) GetTableComment(ctx context.Context, schema, table string) (string, error) {
	query := `
		SELECT obj_description(pc.oid, 'pg_class') as table_comment
		FROM pg_catalog.pg_class pc
		JOIN pg_catalog.pg_namespace pn ON pn.oid = pc.relnamespace
		WHERE pn.nspname = $1 AND pc.relname = $2
	`

	var comment *string
	err := c.QueryRow(ctx, query, schema, table).Scan(&comment)
	if err != nil {
		if err == pgx.ErrNoRows {
			return "", nil
		}
		return "", fmt.Errorf("failed to query table comment: %w", err)
	}

	if comment != nil {
		return *comment, nil
	}
	return "", nil
}

func (c *Client) GetColumnComments(ctx context.Context, schema, table string) (map[string]string, error) {
	query := `
		SELECT
			a.attname as column_name,
			col_description(a.attrelid, a.attnum) as column_comment
		FROM pg_catalog.pg_attribute a
		JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
		JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = $1
		AND c.relname = $2
		AND a.attnum > 0
		AND NOT a.attisdropped
		AND col_description(a.attrelid, a.attnum) IS NOT NULL
		ORDER BY a.attnum
	`

	rows, err := c.Query(ctx, query, schema, table)
	if err != nil {
		return nil, fmt.Errorf("failed to query column comments: %w", err)
	}
	defer rows.Close()

	comments := make(map[string]string)
	for rows.Next() {
		var columnName, comment string
		if err := rows.Scan(&columnName, &comment); err != nil {
			return nil, fmt.Errorf("failed to scan column comment: %w", err)
		}
		comments[columnName] = comment
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating column comments: %w", err)
	}

	return comments, nil
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

type ConstraintInfo struct {
	Name               string
	Type               string
	Columns            []string
	ReferencedTable    *string
	ReferencedColumns  []string
	OnDelete           *string
	OnUpdate           *string
	CheckExpression    *string
	Documentation      *string
}

type IndexInfo struct {
	Name              string
	Columns           []string
	IsUnique          bool
	Type              string
	ConditionExpr     *string
	Documentation     *string
}
