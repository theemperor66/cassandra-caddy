package cassandraadapter

import (
	"encoding/json"
	"fmt"
	"go.uber.org/zap"
	"strconv"
	"strings"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig"
	"github.com/gocql/gocql"
)

// ConfigEntry represents a row in the caddy_config table
type ConfigEntry struct {
	ConfigID    gocql.UUID `json:"-"`
	Path        string     `json:"path"`
	Value       string     `json:"value"`
	DataType    string     `json:"data_type"`
	Enabled     bool       `json:"enabled"`
	LastUpdated time.Time  `json:"last_updated"`
}

func init() {
	caddyconfig.RegisterAdapter("cql", Adapter{})
}

// CassandraAdapterConfig holds the configuration for the Cassandra adapter
type CassandraAdapterConfig struct {
	Hosts        []string `json:"contact_points"`
	Keyspace     string   `json:"keyspace"`
	QueryTimeout int      `json:"query_timeout"`
	LockTimeout  int      `json:"lock_timeout"`
	ConfigID     string   `json:"config_id"` // UUID string for the config to load
}

var (
	arrayFields = map[string]bool{
		"routes":         true,
		"match":          true,
		"handle":         true,
		"listen":         true,
		"Location":       true,
		"host":           true,
		"contact_points": true,
	}

	arrayPaths = map[string]bool{
		"apps.http.servers.*.routes":                true,
		"apps.http.servers.*.routes.*.match":        true,
		"apps.http.servers.*.routes.*.handle":       true,
		"apps.http.servers.*.listen":                true,
		"apps.http.servers.*.routes.*.match.*.host": true,
	}
)

// Adapter implements Caddy's config adapter interface for Cassandra
type Adapter struct{}

// getSession creates a new Cassandra session based on the configuration
func getSession(config CassandraAdapterConfig) (*gocql.Session, error) {
	cluster := gocql.NewCluster(config.Hosts...)
	cluster.Keyspace = config.Keyspace
	cluster.Timeout = time.Duration(config.QueryTimeout) * time.Second

	session, err := cluster.CreateSession()
	if err != nil {
		return nil, fmt.Errorf("failed to create session: %w", err)
	}
	return session, nil
}

// parseValue converts a string value to the appropriate type based on data_type
func parseValue(value string, dataType string) (interface{}, error) {
	// Attempt to unquote JSON strings
	unquoted, unquoteErr := strconv.Unquote(value)
	if unquoteErr == nil {
		value = unquoted
	}

	switch dataType {
	case "string":
		return value, nil

	case "number":
		var num interface{}
		if err := json.Unmarshal([]byte(value), &num); err != nil {
			if n, err := strconv.ParseFloat(value, 64); err == nil {
				return n, nil
			}
			return nil, fmt.Errorf("invalid number value %q: %w", value, err)
		}
		return num, nil

	case "boolean":
		if value == "true" || value == "1" {
			return true, nil
		} else if value == "false" || value == "0" {
			return false, nil
		}
		return nil, fmt.Errorf("invalid boolean value %q", value)

	case "array", "object":
		var result interface{}
		if err := json.Unmarshal([]byte(value), &result); err != nil {
			return nil, fmt.Errorf("failed to unmarshal JSON: %w", err)
		}
		return result, nil

	default:
		return nil, fmt.Errorf("unknown data type %q", dataType)
	}
}

// shouldBeArray determines if a path should be treated as an array
func shouldBeArray(path string) bool {
	parts := strings.Split(path, ".")
	if len(parts) > 0 {
		if arrayFields[parts[len(parts)-1]] {
			return true
		}
	}

	for pattern, isArray := range arrayPaths {
		if isArray && matchWildcardPath(pattern, path) {
			return true
		}
	}
	return false
}

// matchWildcardPath checks if a path matches a pattern with wildcards
func matchWildcardPath(pattern, path string) bool {
	patternParts := strings.Split(pattern, ".")
	pathParts := strings.Split(path, ".")

	if len(patternParts) != len(pathParts) {
		return false
	}

	for i, p := range patternParts {
		if p == "*" {
			continue
		}
		if p != pathParts[i] {
			return false
		}
	}
	return true
}

// setNestedValue builds the configuration structure
func setNestedValue(currentMap map[string]interface{}, path []string, value interface{}) {
	if len(path) == 0 {
		return
	}

	key := path[0]
	if len(path) == 1 {
		if shouldBeArray(strings.Join(path, ".")) {
			if arr, ok := value.([]interface{}); ok {
				currentMap[key] = arr
			} else {
				currentMap[key] = []interface{}{value}
			}
		} else {
			currentMap[key] = value
		}
		return
	}

	// Handle array indexes
	if index, err := strconv.Atoi(path[1]); err == nil {
		var arr []interface{}
		if existing, ok := currentMap[key].([]interface{}); ok {
			arr = existing
		} else {
			arr = make([]interface{}, index+1)
		}

		// Expand array if needed
		for len(arr) <= index {
			arr = append(arr, make(map[string]interface{}))
		}

		if elem, ok := arr[index].(map[string]interface{}); ok {
			setNestedValue(elem, path[2:], value)
		}
		currentMap[key] = arr
		return
	}

	// Handle nested maps
	if _, ok := currentMap[key].(map[string]interface{}); !ok {
		currentMap[key] = make(map[string]interface{})
	}
	if childMap, ok := currentMap[key].(map[string]interface{}); ok {
		setNestedValue(childMap, path[1:], value)
	}
}

// getConfiguration retrieves and builds the configuration from Cassandra
func getConfiguration(session *gocql.Session, configID gocql.UUID) (map[string]interface{}, error) {
	config := make(map[string]interface{})
	iter := session.Query(`
		SELECT path, value, data_type 
		FROM caddy_config 
		WHERE config_id = ? AND enabled = true 
		ALLOW FILTERING`, configID).Iter()

	var entry ConfigEntry
	for iter.Scan(&entry.Path, &entry.Value, &entry.DataType) {
		parsedValue, err := parseValue(entry.Value, entry.DataType)
		if err != nil {
			caddy.Log().Named("adapters.cql").Error("parse error", zap.Error(err))
			continue
		}

		pathParts := strings.Split(entry.Path, ".")
		setNestedValue(config, pathParts, parsedValue)
	}

	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("query iteration failed: %w", err)
	}
	return config, nil
}

// Adapt converts Cassandra-stored configuration to Caddy JSON format
func (a Adapter) Adapt(body []byte, options map[string]interface{}) ([]byte, []caddyconfig.Warning, error) {
	var config CassandraAdapterConfig
	if err := json.Unmarshal(body, &config); err != nil {
		return nil, nil, fmt.Errorf("config unmarshal failed: %w", err)
	}

	if len(config.Hosts) == 0 || config.Keyspace == "" {
		return nil, nil, fmt.Errorf("contact_points and keyspace are required")
	}

	configID, err := gocql.ParseUUID(config.ConfigID)
	if err != nil {
		return nil, nil, fmt.Errorf("invalid config_id: %w", err)
	}

	session, err := getSession(config)
	if err != nil {
		return nil, nil, err
	}
	defer session.Close()

	caddyConfig, err := getConfiguration(session, configID)
	if err != nil {
		return nil, nil, err
	}

	jsonData, err := json.Marshal(caddyConfig)
	if err != nil {
		return nil, nil, fmt.Errorf("config marshal failed: %w", err)
	}

	return jsonData, nil, nil
}

var _ caddyconfig.Adapter = (*Adapter)(nil)
