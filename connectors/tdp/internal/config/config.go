package config

import (
	"fmt"
	"os"
	"strconv"
)

type Config struct {
	values map[string]string
}

func Load() (*Config, error) {
	cfg := &Config{
		values: make(map[string]string),
	}

	cfg.loadFromEnv()
	return cfg, nil
}

func (c *Config) loadFromEnv() {
	envVars := []string{
		"TDP_ENDPOINT",
		"TDP_ACCESS_KEY_ID",
		"TDP_SECRET_ACCESS_KEY",
		"TDP_BUCKET",
		"TDP_PATH",
		"TDP_USE_SSL",
		"TDP_REGION",
		"PORT",
	}

	for _, envVar := range envVars {
		if value := os.Getenv(envVar); value != "" {
			c.values[envVar] = value
		}
	}
}

// NormalizeConfig converts JSON Schema config format to internal format
func NormalizeConfig(rawConfig map[string]string) (map[string]string, error) {
	if err := ValidateConfig(rawConfig); err != nil {
		return nil, err
	}

	config := make(map[string]string)

	// Map JSON Schema field names to internal field names
	if endpoint := rawConfig["endpoint"]; endpoint != "" {
		config["endpoint"] = endpoint
	}
	if accessKeyID := rawConfig["accessKeyId"]; accessKeyID != "" {
		config["access_key_id"] = accessKeyID
	}
	if secretAccessKey := rawConfig["secretAccessKey"]; secretAccessKey != "" {
		config["secret_access_key"] = secretAccessKey
	}
	if bucket := rawConfig["bucket"]; bucket != "" {
		config["bucket"] = bucket
	}
	if tdpPath := rawConfig["tdpPath"]; tdpPath != "" {
		config["tdp_path"] = tdpPath
	}

	// Optional fields with defaults
	if useSsl := rawConfig["useSsl"]; useSsl != "" {
		config["use_ssl"] = useSsl
	} else {
		config["use_ssl"] = "true"
	}
	if region := rawConfig["region"]; region != "" {
		config["region"] = region
	} else {
		config["region"] = "us-east-1"
	}

	return config, nil
}

// ValidateConfig validates required fields are present
func ValidateConfig(config map[string]string) error {
	requiredFields := []string{"endpoint", "accessKeyId", "secretAccessKey", "bucket", "tdpPath"}

	for _, field := range requiredFields {
		if value := config[field]; value == "" {
			return fmt.Errorf("required field '%s' is missing or empty", field)
		}
	}

	return nil
}

func (c *Config) GetString(key, defaultValue string) string {
	if value, exists := c.values[key]; exists {
		return value
	}
	return defaultValue
}

func (c *Config) GetBool(key string, defaultValue bool) bool {
	if value, exists := c.values[key]; exists {
		if boolValue, err := strconv.ParseBool(value); err == nil {
			return boolValue
		}
	}
	return defaultValue
}

func (c *Config) GetS3Config() map[string]string {
	return map[string]string{
		"endpoint":          c.GetString("TDP_ENDPOINT", ""),
		"access_key_id":     c.GetString("TDP_ACCESS_KEY_ID", ""),
		"secret_access_key": c.GetString("TDP_SECRET_ACCESS_KEY", ""),
		"bucket":            c.GetString("TDP_BUCKET", ""),
		"tdp_path":          c.GetString("TDP_PATH", ""),
		"use_ssl":           c.GetString("TDP_USE_SSL", "true"),
		"region":            c.GetString("TDP_REGION", "us-east-1"),
	}
}
