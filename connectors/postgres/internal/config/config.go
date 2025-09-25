package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
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
		"POSTGRES_HOST",
		"POSTGRES_PORT",
		"POSTGRES_DATABASE",
		"POSTGRES_USERNAME",
		"POSTGRES_PASSWORD",
		"POSTGRES_SSLMODE",
		"POSTGRES_SCHEMA",
		"POSTGRES_CONNECT_TIMEOUT",
		"POSTGRES_STATEMENT_TIMEOUT",
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
	if hostname := rawConfig["hostname"]; hostname != "" {
		config["host"] = hostname
	}
	if port := rawConfig["port"]; port != "" {
		config["port"] = port
	}
	if database := rawConfig["database"]; database != "" {
		config["database"] = database
	}
	if username := rawConfig["username"]; username != "" {
		config["username"] = username
	}
	if password := rawConfig["password"]; password != "" {
		config["password"] = password
	}
	if sslMode := rawConfig["sslMode"]; sslMode != "" {
		config["sslmode"] = sslMode
	} else {
		config["sslmode"] = "prefer" // default value
	}
	if schema := rawConfig["schema"]; schema != "" {
		config["schema"] = schema
	} else {
		config["schema"] = "public" // default value
	}

	// Convert integer timeouts to duration strings
	if connectTimeout := rawConfig["connectTimeout"]; connectTimeout != "" {
		config["connect_timeout"] = connectTimeout + "s"
	} else {
		config["connect_timeout"] = "30s" // default value
	}
	if statementTimeout := rawConfig["statementTimeout"]; statementTimeout != "" {
		config["statement_timeout"] = statementTimeout + "s"
	} else {
		config["statement_timeout"] = "300s" // default value
	}

	return config, nil
}

// ValidateConfig validates required fields are present
func ValidateConfig(config map[string]string) error {
	requiredFields := []string{"hostname", "port", "database", "username", "password"}

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

func (c *Config) GetInt(key string, defaultValue int) int {
	if value, exists := c.values[key]; exists {
		if intValue, err := strconv.Atoi(value); err == nil {
			return intValue
		}
	}
	return defaultValue
}

func (c *Config) GetDuration(key string, defaultValue time.Duration) time.Duration {
	if value, exists := c.values[key]; exists {
		if duration, err := time.ParseDuration(value); err == nil {
			return duration
		}
	}
	return defaultValue
}

func (c *Config) GetConnectionConfig() map[string]string {
	config := map[string]string{
		"host":              c.GetString("POSTGRES_HOST", "localhost"),
		"port":              c.GetString("POSTGRES_PORT", "5432"),
		"database":          c.GetString("POSTGRES_DATABASE", ""),
		"username":          c.GetString("POSTGRES_USERNAME", ""),
		"password":          c.GetString("POSTGRES_PASSWORD", ""),
		"sslmode":           c.GetString("POSTGRES_SSLMODE", "prefer"),
		"schema":            c.GetString("POSTGRES_SCHEMA", "public"),
		"connect_timeout":   c.GetString("POSTGRES_CONNECT_TIMEOUT", "30s"),
		"statement_timeout": c.GetString("POSTGRES_STATEMENT_TIMEOUT", "300s"),
	}

	for k, v := range c.values {
		if v != "" {
			switch k {
			case "POSTGRES_HOST":
				config["host"] = v
			case "POSTGRES_PORT":
				config["port"] = v
			case "POSTGRES_DATABASE":
				config["database"] = v
			case "POSTGRES_USERNAME":
				config["username"] = v
			case "POSTGRES_PASSWORD":
				config["password"] = v
			case "POSTGRES_SSLMODE":
				config["sslmode"] = v
			case "POSTGRES_SCHEMA":
				config["schema"] = v
			case "POSTGRES_CONNECT_TIMEOUT":
				config["connect_timeout"] = v
			case "POSTGRES_STATEMENT_TIMEOUT":
				config["statement_timeout"] = v
			}
		}
	}

	return config
}
