// Package logging provides structured logging utilities for connectors
package logging

import (
	"context"
	"os"

	"go.uber.org/zap"
)

// ConnectorLogger wraps zap.Logger with connector-specific functionality
type ConnectorLogger struct {
	*zap.Logger
	fields map[string]interface{}
}

// Config holds logging configuration
type Config struct {
	Level       string            `json:"level"`
	Format      string            `json:"format"` // "json" or "console"
	OutputPath  string            `json:"output_path"`
	Fields      map[string]string `json:"fields"`
	Development bool              `json:"development"`
}

// NewLogger creates a new structured logger for connectors
func NewLogger(config Config) (*ConnectorLogger, error) {
	var zapConfig zap.Config

	if config.Development {
		zapConfig = zap.NewDevelopmentConfig()
	} else {
		zapConfig = zap.NewProductionConfig()
	}

	// Set log level
	level, err := zap.ParseAtomicLevel(config.Level)
	if err != nil {
		level = zap.NewAtomicLevelAt(zap.InfoLevel)
	}
	zapConfig.Level = level

	// Set output format
	if config.Format == "console" {
		zapConfig.Encoding = "console"
	} else {
		zapConfig.Encoding = "json"
	}

	// Set output path
	if config.OutputPath != "" {
		zapConfig.OutputPaths = []string{config.OutputPath}
	}

	logger, err := zapConfig.Build()
	if err != nil {
		return nil, err
	}

	// Add default fields
	fields := make(map[string]interface{})
	for k, v := range config.Fields {
		fields[k] = v
	}

	return &ConnectorLogger{
		Logger: logger,
		fields: fields,
	}, nil
}

// NewDefaultLogger creates a logger with sensible defaults
func NewDefaultLogger() *ConnectorLogger {
	config := Config{
		Level:       "info",
		Format:      "json",
		Development: false,
		Fields: map[string]string{
			"service": "connector",
		},
	}

	logger, err := NewLogger(config)
	if err != nil {
		// Fallback to basic logger
		zapLogger, _ := zap.NewProduction()
		return &ConnectorLogger{
			Logger: zapLogger,
			fields: map[string]interface{}{"service": "connector"},
		}
	}

	return logger
}

// WithField adds a field to the logger context
func (l *ConnectorLogger) WithField(key string, value interface{}) *ConnectorLogger {
	newFields := make(map[string]interface{})
	for k, v := range l.fields {
		newFields[k] = v
	}
	newFields[key] = value

	return &ConnectorLogger{
		Logger: l.Logger.With(zap.Any(key, value)),
		fields: newFields,
	}
}

// WithFields adds multiple fields to the logger context
func (l *ConnectorLogger) WithFields(fields map[string]interface{}) *ConnectorLogger {
	newFields := make(map[string]interface{})
	for k, v := range l.fields {
		newFields[k] = v
	}
	for k, v := range fields {
		newFields[k] = v
	}

	zapFields := make([]zap.Field, 0, len(fields))
	for k, v := range fields {
		zapFields = append(zapFields, zap.Any(k, v))
	}

	return &ConnectorLogger{
		Logger: l.Logger.With(zapFields...),
		fields: newFields,
	}
}

// WithContext extracts tracing information from context if available
func (l *ConnectorLogger) WithContext(ctx context.Context) *ConnectorLogger {
	// TODO: Add distributed tracing integration (OpenTelemetry)
	return l
}

// LogConnectorEvent logs a connector-specific event
func (l *ConnectorLogger) LogConnectorEvent(event string, fields map[string]interface{}) {
	allFields := map[string]interface{}{
		"event": event,
	}
	for k, v := range fields {
		allFields[k] = v
	}

	l.WithFields(allFields).Info("Connector event")
}

// LogPerformanceMetric logs performance-related metrics
func (l *ConnectorLogger) LogPerformanceMetric(metric string, value interface{}, unit string) {
	l.WithFields(map[string]interface{}{
		"metric": metric,
		"value":  value,
		"unit":   unit,
		"type":   "performance",
	}).Info("Performance metric")
}

// LogDataQualityEvent logs data quality issues
func (l *ConnectorLogger) LogDataQualityEvent(entity string, issue string, severity string) {
	l.WithFields(map[string]interface{}{
		"entity":   entity,
		"issue":    issue,
		"severity": severity,
		"type":     "data_quality",
	}).Warn("Data quality issue")
}

// Sync flushes any buffered log entries
func (l *ConnectorLogger) Sync() error {
	return l.Logger.Sync()
}
