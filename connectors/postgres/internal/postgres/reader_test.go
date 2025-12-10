package postgres

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	noesisv1 "github.com/data-power-io/noesis-protocol/languages/go/datapower/noesis/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

const (
	testSchema = "public"
	testTable  = "test_data_types"
)

// TestConvertRowToRecord_DataTypes validates that PostgreSQL data types
// are correctly mapped to Noesis protocol Value types
func TestConvertRowToRecord_DataTypes(t *testing.T) {
	// Skip if no test database is available
	config := getTestConfig(t)
	if config == nil {
		t.Skip("No test database configured - set TEST_PG_HOST, TEST_PG_USER, etc.")
	}

	ctx := context.Background()
	logger := zap.NewNop()

	// Create client
	client, err := NewClient(config, logger)
	require.NoError(t, err, "Failed to create client")
	defer client.Close()

	// Verify connection
	err = client.Ping(ctx)
	require.NoError(t, err, "Failed to ping database")

	// Setup: Create test table
	setupTestTable(t, ctx, client)
	defer cleanupTestTable(t, ctx, client)

	// Insert test data
	insertTestData(t, ctx, client)

	// Create reader
	reader, err := NewReader(client, config, logger)
	require.NoError(t, err, "Failed to create reader")

	// Get columns metadata
	columns, err := client.GetColumns(ctx, testSchema, testTable)
	require.NoError(t, err, "Failed to get columns")
	require.NotEmpty(t, columns, "No columns returned")

	// Query test data
	query := fmt.Sprintf(`SELECT * FROM "%s"."%s" ORDER BY id`, testSchema, testTable)
	rows, err := client.Query(ctx, query)
	require.NoError(t, err, "Failed to query test data")
	defer rows.Close()

	// Read first row
	require.True(t, rows.Next(), "No rows returned")
	values, err := rows.Values()
	require.NoError(t, err, "Failed to scan row")

	// Convert to record
	record, err := reader.convertRowToRecord(values, columns, testTable)
	require.NoError(t, err, "Failed to convert row to record")
	require.NotNil(t, record, "Record is nil")
	require.NotNil(t, record.Data, "Record data is nil")

	// Validate entity and operation
	assert.Equal(t, testTable, record.Entity)
	assert.Equal(t, noesisv1.Op_UPSERT, record.Op)

	// Validate each column type
	t.Run("INTEGER_maps_to_Int32Val", func(t *testing.T) {
		val := record.Data.Columns["id"]
		require.NotNil(t, val, "id column is nil")

		int32Val, ok := val.Kind.(*noesisv1.Value_Int32Val)
		assert.True(t, ok, "id should be Int32Val, got %T", val.Kind)
		if ok {
			assert.Equal(t, int32(1), int32Val.Int32Val)
		}
	})

	t.Run("BIGINT_maps_to_Int64Val", func(t *testing.T) {
		val := record.Data.Columns["big_number"]
		require.NotNil(t, val, "big_number column is nil")

		int64Val, ok := val.Kind.(*noesisv1.Value_Int64Val)
		assert.True(t, ok, "big_number should be Int64Val, got %T", val.Kind)
		if ok {
			assert.Equal(t, int64(9223372036854775807), int64Val.Int64Val)
		}
	})

	t.Run("SMALLINT_maps_to_Int32Val", func(t *testing.T) {
		val := record.Data.Columns["small_number"]
		require.NotNil(t, val, "small_number column is nil")

		int32Val, ok := val.Kind.(*noesisv1.Value_Int32Val)
		assert.True(t, ok, "small_number should be Int32Val, got %T", val.Kind)
		if ok {
			assert.Equal(t, int32(100), int32Val.Int32Val)
		}
	})

	t.Run("VARCHAR_maps_to_StringVal", func(t *testing.T) {
		val := record.Data.Columns["name"]
		require.NotNil(t, val, "name column is nil")

		stringVal, ok := val.Kind.(*noesisv1.Value_StringVal)
		assert.True(t, ok, "name should be StringVal, got %T", val.Kind)
		if ok {
			assert.Equal(t, "Test Product", stringVal.StringVal)
		}
	})

	t.Run("BOOLEAN_maps_to_BoolVal", func(t *testing.T) {
		val := record.Data.Columns["is_active"]
		require.NotNil(t, val, "is_active column is nil")

		boolVal, ok := val.Kind.(*noesisv1.Value_BoolVal)
		assert.True(t, ok, "is_active should be BoolVal, got %T", val.Kind)
		if ok {
			assert.Equal(t, true, boolVal.BoolVal)
		}
	})

	t.Run("REAL_maps_to_FloatVal", func(t *testing.T) {
		val := record.Data.Columns["weight"]
		require.NotNil(t, val, "weight column is nil")

		floatVal, ok := val.Kind.(*noesisv1.Value_FloatVal)
		assert.True(t, ok, "weight should be FloatVal, got %T", val.Kind)
		if ok {
			assert.InDelta(t, float32(2.5), floatVal.FloatVal, 0.001)
		}
	})

	t.Run("DOUBLE_PRECISION_maps_to_DoubleVal", func(t *testing.T) {
		val := record.Data.Columns["rating"]
		require.NotNil(t, val, "rating column is nil")

		doubleVal, ok := val.Kind.(*noesisv1.Value_DoubleVal)
		assert.True(t, ok, "rating should be DoubleVal, got %T", val.Kind)
		if ok {
			assert.InDelta(t, 4.75, doubleVal.DoubleVal, 0.001)
		}
	})

	t.Run("TIMESTAMP_maps_to_TimestampMicros", func(t *testing.T) {
		val := record.Data.Columns["created_at"]
		require.NotNil(t, val, "created_at column is nil")

		timestampVal, ok := val.Kind.(*noesisv1.Value_TimestampMicros)
		assert.True(t, ok, "created_at should be TimestampMicros, got %T", val.Kind)
		if ok {
			// Just verify it's a reasonable timestamp (not zero)
			assert.Greater(t, timestampVal.TimestampMicros, int64(0))
		}
	})

	t.Run("TEXT_maps_to_StringVal", func(t *testing.T) {
		val := record.Data.Columns["notes"]
		require.NotNil(t, val, "notes column is nil")

		stringVal, ok := val.Kind.(*noesisv1.Value_StringVal)
		assert.True(t, ok, "notes should be StringVal, got %T", val.Kind)
		if ok {
			assert.Equal(t, "Test notes", stringVal.StringVal)
		}
	})
}

// TestConvertRowToRecord_NullValues validates that NULL values are correctly handled
func TestConvertRowToRecord_NullValues(t *testing.T) {
	config := getTestConfig(t)
	if config == nil {
		t.Skip("No test database configured")
	}

	ctx := context.Background()
	logger := zap.NewNop()

	client, err := NewClient(config, logger)
	require.NoError(t, err)
	defer client.Close()

	setupTestTable(t, ctx, client)
	defer cleanupTestTable(t, ctx, client)

	// Insert row with NULL values
	query := fmt.Sprintf(`
		INSERT INTO "%s"."%s" (id, name)
		VALUES (999, 'Null Test')
	`, testSchema, testTable)
	_, err = client.pool.Exec(ctx, query)
	require.NoError(t, err, "Failed to insert null test data")

	reader, err := NewReader(client, config, logger)
	require.NoError(t, err)

	columns, err := client.GetColumns(ctx, testSchema, testTable)
	require.NoError(t, err)

	query = fmt.Sprintf(`SELECT * FROM "%s"."%s" WHERE id = 999`, testSchema, testTable)
	rows, err := client.Query(ctx, query)
	require.NoError(t, err)
	defer rows.Close()

	require.True(t, rows.Next())
	values, err := rows.Values()
	require.NoError(t, err)

	record, err := reader.convertRowToRecord(values, columns, testTable)
	require.NoError(t, err)

	// Verify NULL values are represented as NullVal
	t.Run("NULL_INTEGER_is_NullVal", func(t *testing.T) {
		val := record.Data.Columns["big_number"]
		require.NotNil(t, val)
		_, ok := val.Kind.(*noesisv1.Value_NullVal)
		assert.True(t, ok, "NULL big_number should be NullVal, got %T", val.Kind)
	})

	t.Run("NULL_STRING_is_NullVal", func(t *testing.T) {
		val := record.Data.Columns["notes"]
		require.NotNil(t, val)
		_, ok := val.Kind.(*noesisv1.Value_NullVal)
		assert.True(t, ok, "NULL notes should be NullVal, got %T", val.Kind)
	})
}

// Helper functions

func getTestConfig(t *testing.T) map[string]string {
	// Try to connect to test database from docker-compose
	// You can override these with environment variables
	host := getEnvOrDefault("TEST_PG_HOST", "localhost")
	port := getEnvOrDefault("TEST_PG_PORT", "5432")
	username := getEnvOrDefault("TEST_PG_USER", "postgres")
	password := getEnvOrDefault("TEST_PG_PASSWORD", "postgres")
	database := getEnvOrDefault("TEST_PG_DATABASE", "noesis")

	return map[string]string{
		"host":     host,
		"port":     port,
		"username": username,
		"password": password,
		"database": database,
		"schema":   testSchema,
		"sslmode":  "disable",
	}
}

func getEnvOrDefault(key, defaultValue string) string {
	if val := getEnv(key); val != "" {
		return val
	}
	return defaultValue
}

func getEnv(key string) string {
	return os.Getenv(key)
}

func setupTestTable(t *testing.T, ctx context.Context, client *Client) {
	// Drop table if exists
	dropQuery := fmt.Sprintf(`DROP TABLE IF EXISTS "%s"."%s"`, testSchema, testTable)
	_, err := client.pool.Exec(ctx, dropQuery)
	require.NoError(t, err, "Failed to drop test table")

	// Create test table with various data types
	createQuery := fmt.Sprintf(`
		CREATE TABLE "%s"."%s" (
			id INTEGER PRIMARY KEY,
			big_number BIGINT,
			small_number SMALLINT,
			name VARCHAR(100),
			is_active BOOLEAN,
			price NUMERIC(10,2),
			weight REAL,
			rating DOUBLE PRECISION,
			created_at TIMESTAMP,
			notes TEXT
		)
	`, testSchema, testTable)

	_, err = client.pool.Exec(ctx, createQuery)
	require.NoError(t, err, "Failed to create test table")
}

func cleanupTestTable(t *testing.T, ctx context.Context, client *Client) {
	query := fmt.Sprintf(`DROP TABLE IF EXISTS "%s"."%s"`, testSchema, testTable)
	_, err := client.pool.Exec(ctx, query)
	if err != nil {
		t.Logf("Warning: Failed to cleanup test table: %v", err)
	}
}

func insertTestData(t *testing.T, ctx context.Context, client *Client) {
	query := fmt.Sprintf(`
		INSERT INTO "%s"."%s"
		(id, big_number, small_number, name, is_active, price, weight, rating, created_at, notes)
		VALUES
		(1, 9223372036854775807, 100, 'Test Product', true, 99.99, 2.5, 4.75, $1, 'Test notes')
	`, testSchema, testTable)

	now := time.Now()
	_, err := client.pool.Exec(ctx, query, now)
	require.NoError(t, err, "Failed to insert test data")
}
