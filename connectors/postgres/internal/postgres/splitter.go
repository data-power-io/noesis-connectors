package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"math"

	noesisv1 "github.com/data-power-io/noesis-protocol/languages/go/datapower/noesis/v1"
	"go.uber.org/zap"
)

// SplitStrategy defines the strategy used to split a table
type SplitStrategy string

const (
	SplitStrategyAuto       SplitStrategy = "auto"
	SplitStrategyPrimaryKey SplitStrategy = "primary_key"
	SplitStrategyRowNumber  SplitStrategy = "row_number"
	SplitStrategyModulo     SplitStrategy = "modulo"
)

// Default split configuration
const (
	DefaultTargetSplitSize = 1000000 // 1 million rows per split
	DefaultMinSplits       = 1
	DefaultMaxSplits       = 32
)

// SplitToken contains the metadata needed to read a specific split
type SplitToken struct {
	Strategy   SplitStrategy `json:"strategy"`
	Column     string        `json:"column,omitempty"`
	MinValue   interface{}   `json:"min_value,omitempty"`
	MaxValue   interface{}   `json:"max_value,omitempty"`
	SplitIndex int           `json:"split_index"`
	TotalCount int           `json:"total_count,omitempty"`
	Offset     int64         `json:"offset,omitempty"`
	Limit      int64         `json:"limit,omitempty"`
	Modulo     int           `json:"modulo,omitempty"`
}

// TableStats contains statistics about a table
type TableStats struct {
	RowCount     int64
	HasPrimaryKey bool
	PrimaryKeyColumn string
	PrimaryKeyType string
}

// Splitter handles split planning for parallel extraction
type Splitter struct {
	client *Client
	logger *zap.Logger
}

// NewSplitter creates a new Splitter instance
func NewSplitter(client *Client, logger *zap.Logger) *Splitter {
	return &Splitter{
		client: client,
		logger: logger,
	}
}

// GenerateSplits generates extraction splits for a table
func (s *Splitter) GenerateSplits(ctx context.Context, schema, entity string, desiredParallelism int32) ([]*noesisv1.ExtractionSplit, int64, error) {
	// Get table statistics
	stats, err := s.analyzeTable(ctx, schema, entity)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to analyze table: %w", err)
	}

	// If table is empty or very small, return single split
	if stats.RowCount == 0 {
		return s.createSingleSplit(stats.RowCount)
	}

	// Determine actual number of splits
	numSplits := s.calculateNumSplits(stats.RowCount, desiredParallelism)
	if numSplits == 1 {
		return s.createSingleSplit(stats.RowCount)
	}

	// Choose strategy
	strategy := s.chooseStrategy(stats)

	s.logger.Info("Generating splits",
		zap.String("entity", entity),
		zap.String("strategy", string(strategy)),
		zap.Int("num_splits", numSplits),
		zap.Int64("row_count", stats.RowCount))

	// Generate splits based on strategy
	switch strategy {
	case SplitStrategyPrimaryKey:
		return s.generatePrimaryKeySplits(ctx, schema, entity, stats, numSplits)
	case SplitStrategyRowNumber:
		return s.generateRowNumberSplits(stats.RowCount, numSplits)
	case SplitStrategyModulo:
		return s.generateModuloSplits(stats, numSplits)
	default:
		return nil, 0, fmt.Errorf("unsupported split strategy: %s", strategy)
	}
}

// analyzeTable analyzes a table to determine splitting strategy
func (s *Splitter) analyzeTable(ctx context.Context, schema, entity string) (*TableStats, error) {
	stats := &TableStats{}

	// Get row count estimate
	rowCount, err := s.client.GetTableRowCount(ctx, schema, entity)
	if err != nil {
		return nil, fmt.Errorf("failed to get row count: %w", err)
	}
	stats.RowCount = rowCount

	// Get primary key information
	pkInfo, err := s.client.GetPrimaryKey(ctx, schema, entity)
	if err != nil {
		s.logger.Debug("No primary key found", zap.Error(err))
	} else if pkInfo != nil {
		stats.HasPrimaryKey = true
		stats.PrimaryKeyColumn = pkInfo.Column
		stats.PrimaryKeyType = pkInfo.DataType
	}

	return stats, nil
}

// chooseStrategy selects the best splitting strategy
func (s *Splitter) chooseStrategy(stats *TableStats) SplitStrategy {
	// Prefer primary key splitting for integer PKs
	if stats.HasPrimaryKey {
		switch stats.PrimaryKeyType {
		case "integer", "int4", "bigint", "int8", "smallint", "int2":
			return SplitStrategyPrimaryKey
		}
	}

	// Fallback to row number splitting
	return SplitStrategyRowNumber
}

// calculateNumSplits determines the optimal number of splits
func (s *Splitter) calculateNumSplits(rowCount int64, desiredParallelism int32) int {
	if rowCount <= DefaultTargetSplitSize {
		return 1
	}

	// Calculate based on target split size
	calculatedSplits := int(math.Ceil(float64(rowCount) / float64(DefaultTargetSplitSize)))

	// Use desired parallelism if specified
	if desiredParallelism > 0 {
		calculatedSplits = int(desiredParallelism)
	}

	// Enforce limits
	if calculatedSplits < DefaultMinSplits {
		calculatedSplits = DefaultMinSplits
	}
	if calculatedSplits > DefaultMaxSplits {
		calculatedSplits = DefaultMaxSplits
	}

	return calculatedSplits
}

// createSingleSplit creates a single split for the entire table
func (s *Splitter) createSingleSplit(rowCount int64) ([]*noesisv1.ExtractionSplit, int64, error) {
	token := &SplitToken{
		Strategy:   SplitStrategyRowNumber,
		SplitIndex: 0,
	}

	tokenBytes, err := json.Marshal(token)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to marshal split token: %w", err)
	}

	split := &noesisv1.ExtractionSplit{
		SplitId:       "split-0000",
		SplitToken:    tokenBytes,
		EstimatedRows: rowCount,
		Metadata: map[string]string{
			"strategy":    string(SplitStrategyRowNumber),
			"split_index": "0",
		},
	}

	return []*noesisv1.ExtractionSplit{split}, rowCount, nil
}

// generatePrimaryKeySplits generates splits based on primary key ranges
func (s *Splitter) generatePrimaryKeySplits(ctx context.Context, schema, entity string, stats *TableStats, numSplits int) ([]*noesisv1.ExtractionSplit, int64, error) {
	// Get min and max values of primary key
	minValue, maxValue, err := s.client.GetMinMaxValue(ctx, schema, entity, stats.PrimaryKeyColumn)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to get min/max values: %w", err)
	}

	// Convert to int64 for range calculation
	min, ok1 := toInt64(minValue)
	max, ok2 := toInt64(maxValue)
	if !ok1 || !ok2 {
		s.logger.Warn("Failed to convert PK values to int64, falling back to row number strategy")
		return s.generateRowNumberSplits(stats.RowCount, numSplits)
	}

	if min >= max {
		return s.createSingleSplit(stats.RowCount)
	}

	rangeSize := (max - min + 1) / int64(numSplits)
	if rangeSize == 0 {
		rangeSize = 1
	}

	splits := make([]*noesisv1.ExtractionSplit, 0, numSplits)
	estimatedRowsPerSplit := stats.RowCount / int64(numSplits)

	for i := 0; i < numSplits; i++ {
		rangeMin := min + int64(i)*rangeSize
		rangeMax := rangeMin + rangeSize

		// Last split includes remainder
		if i == numSplits-1 {
			rangeMax = max + 1 // Exclusive upper bound
		}

		token := &SplitToken{
			Strategy:   SplitStrategyPrimaryKey,
			Column:     stats.PrimaryKeyColumn,
			MinValue:   rangeMin,
			MaxValue:   rangeMax,
			SplitIndex: i,
		}

		tokenBytes, err := json.Marshal(token)
		if err != nil {
			return nil, 0, fmt.Errorf("failed to marshal split token: %w", err)
		}

		split := &noesisv1.ExtractionSplit{
			SplitId:       fmt.Sprintf("split-%04d", i),
			SplitToken:    tokenBytes,
			EstimatedRows: estimatedRowsPerSplit,
			Metadata: map[string]string{
				"strategy":    string(SplitStrategyPrimaryKey),
				"column":      stats.PrimaryKeyColumn,
				"min_value":   fmt.Sprintf("%v", rangeMin),
				"max_value":   fmt.Sprintf("%v", rangeMax),
				"split_index": fmt.Sprintf("%d", i),
			},
		}

		splits = append(splits, split)
	}

	return splits, stats.RowCount, nil
}

// generateRowNumberSplits generates splits based on row number ranges using OFFSET/LIMIT
func (s *Splitter) generateRowNumberSplits(rowCount int64, numSplits int) ([]*noesisv1.ExtractionSplit, int64, error) {
	splits := make([]*noesisv1.ExtractionSplit, 0, numSplits)
	rowsPerSplit := rowCount / int64(numSplits)

	for i := 0; i < numSplits; i++ {
		offset := int64(i) * rowsPerSplit
		limit := rowsPerSplit

		// Last split includes remainder
		if i == numSplits-1 {
			limit = rowCount - offset
		}

		token := &SplitToken{
			Strategy:   SplitStrategyRowNumber,
			Offset:     offset,
			Limit:      limit,
			SplitIndex: i,
		}

		tokenBytes, err := json.Marshal(token)
		if err != nil {
			return nil, 0, fmt.Errorf("failed to marshal split token: %w", err)
		}

		split := &noesisv1.ExtractionSplit{
			SplitId:       fmt.Sprintf("split-%04d", i),
			SplitToken:    tokenBytes,
			EstimatedRows: limit,
			Metadata: map[string]string{
				"strategy":    string(SplitStrategyRowNumber),
				"offset":      fmt.Sprintf("%d", offset),
				"limit":       fmt.Sprintf("%d", limit),
				"split_index": fmt.Sprintf("%d", i),
			},
		}

		splits = append(splits, split)
	}

	return splits, rowCount, nil
}

// generateModuloSplits generates splits based on modulo hashing
func (s *Splitter) generateModuloSplits(stats *TableStats, numSplits int) ([]*noesisv1.ExtractionSplit, int64, error) {
	if !stats.HasPrimaryKey {
		return nil, 0, fmt.Errorf("modulo strategy requires a primary key")
	}

	splits := make([]*noesisv1.ExtractionSplit, 0, numSplits)
	estimatedRowsPerSplit := stats.RowCount / int64(numSplits)

	for i := 0; i < numSplits; i++ {
		token := &SplitToken{
			Strategy:   SplitStrategyModulo,
			Column:     stats.PrimaryKeyColumn,
			Modulo:     numSplits,
			SplitIndex: i,
		}

		tokenBytes, err := json.Marshal(token)
		if err != nil {
			return nil, 0, fmt.Errorf("failed to marshal split token: %w", err)
		}

		split := &noesisv1.ExtractionSplit{
			SplitId:       fmt.Sprintf("split-%04d", i),
			SplitToken:    tokenBytes,
			EstimatedRows: estimatedRowsPerSplit,
			Metadata: map[string]string{
				"strategy":    string(SplitStrategyModulo),
				"column":      stats.PrimaryKeyColumn,
				"modulo":      fmt.Sprintf("%d", numSplits),
				"split_index": fmt.Sprintf("%d", i),
			},
		}

		splits = append(splits, split)
	}

	return splits, stats.RowCount, nil
}

// toInt64 attempts to convert a value to int64
func toInt64(v interface{}) (int64, bool) {
	switch val := v.(type) {
	case int:
		return int64(val), true
	case int32:
		return int64(val), true
	case int64:
		return val, true
	case float64:
		return int64(val), true
	default:
		return 0, false
	}
}
