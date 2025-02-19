package cassandraadapter

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig"
	"github.com/gocql/gocql"
)

// ConfigEntry represents a row in the caddy_config table.
type ConfigEntry struct {
	Path        string    `json:"path"`
	Value       string    `json:"value"`
	DataType    string    `json:"data_type"`
	Enabled     bool      `json:"enabled"`
	LastUpdated time.Time `json:"last_updated"`
}

func init() {
	caddyconfig.RegisterAdapter("cql", Adapter{})
}

// CassandraAdapterConfig holds the configuration for the Cassandra adapter.
type CassandraAdapterConfig struct {
	Hosts        []string `json:"contact_points"`
	Keyspace     string   `json:"keyspace"`
	QueryTimeout int      `json:"query_timeout"`
	// Optionally, you can add a PageSize and WorkerCount for tuning:
	PageSize    int `json:"page_size"`
	WorkerCount int `json:"worker_count"`
}

var (
	// arrayFields defines keys that should be interpreted as arrays.
	arrayFields = map[string]bool{
		"routes":         true,
		"match":          true,
		"handle":         true,
		"listen":         true,
		"Location":       true,
		"host":           true,
		"contact_points": true,
	}

	// arrayPaths is used to check specific patterns for array types.
	arrayPaths = map[string]bool{
		"apps.http.servers.*.routes.*.match.*.host": true,
	}
)

// Adapter implements Caddy's config adapter interface for Cassandra.
type Adapter struct{}

// getSession creates a new Cassandra session based on the adapter configuration.
func getSession(config CassandraAdapterConfig) (*gocql.Session, error) {
	cluster := gocql.NewCluster(config.Hosts...)
	cluster.Keyspace = config.Keyspace
	cluster.Timeout = time.Duration(config.QueryTimeout) * time.Second

	session, err := cluster.CreateSession()
	if err != nil {
		return nil, fmt.Errorf("failed to create Cassandra session: %w", err)
	}
	caddy.Log().Named("adapters.cql").Info("Cassandra session established",
		zap.Strings("hosts", config.Hosts),
		zap.String("keyspace", config.Keyspace))
	return session, nil
}

// parseValue converts a string value to the appropriate Go type based on data_type.
// For data_type "array", if the value is not valid JSON, it is automatically wrapped in an array.
func parseValue(value string, dataType string) (interface{}, error) {
	// Attempt to unquote JSON strings (if quoted).
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
			if n, err2 := strconv.ParseFloat(value, 64); err2 == nil {
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

	case "array":
		var result interface{}
		// First, try to unmarshal as JSON.
		if err := json.Unmarshal([]byte(value), &result); err != nil {
			// Log a warning and attempt to wrap the bare value in an array.
			caddy.Log().Named("adapters.cql").Warn("Failed to unmarshal value as array; wrapping in array",
				zap.String("value", value),
				zap.Error(err))
			wrapped := fmt.Sprintf(`["%s"]`, value)
			if err2 := json.Unmarshal([]byte(wrapped), &result); err2 != nil {
				return nil, fmt.Errorf("failed to unmarshal value as array after wrapping: %w", err2)
			}
			return result, nil
		}
		return result, nil

	case "object":
		var result interface{}
		if err := json.Unmarshal([]byte(value), &result); err != nil {
			return nil, fmt.Errorf("failed to unmarshal JSON as object: %w", err)
		}
		return result, nil

	default:
		return nil, fmt.Errorf("unknown data type %q", dataType)
	}
}

// shouldBeArray returns true if a given path should be interpreted as an array.
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

// matchWildcardPath checks if a given path matches a pattern with wildcards.
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

// setNestedValue recursively builds the configuration structure based on a dot-separated path.
func setNestedValue(currentMap map[string]interface{}, path []string, value interface{}) {
	if len(path) == 0 {
		return
	}

	key := path[0]
	// Base case: assign value at the final key.
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

	// Check if the next part of the path is an array index.
	if index, err := strconv.Atoi(path[1]); err == nil {
		var arr []interface{}
		if existing, ok := currentMap[key].([]interface{}); ok {
			arr = existing
		} else {
			arr = make([]interface{}, index+1)
		}

		// Expand the array if necessary.
		for len(arr) <= index {
			arr = append(arr, nil)
		}

		// Initialize the array element if it's nil.
		if arr[index] == nil {
			arr[index] = make(map[string]interface{})
		}

		// Recursively set the nested value.
		if elem, ok := arr[index].(map[string]interface{}); ok {
			setNestedValue(elem, path[2:], value)
		} else {
			caddy.Log().Named("adapters.cql").Error("Unexpected type at array index",
				zap.String("key", key),
				zap.Int("index", index),
				zap.Any("element", arr[index]))
		}
		currentMap[key] = arr
		return
	}

	// Otherwise, treat as a nested map.
	if _, ok := currentMap[key].(map[string]interface{}); !ok {
		currentMap[key] = make(map[string]interface{})
	}
	if childMap, ok := currentMap[key].(map[string]interface{}); ok {
		setNestedValue(childMap, path[1:], value)
	}
}

// rowEntry holds a single row from the Cassandra query.
type rowEntry struct {
	path     string
	value    string
	dataType string
}

// getConfiguration retrieves and assembles the configuration from Cassandra.
// It uses paging and a worker pool to process rows concurrently.
func getConfiguration(session *gocql.Session, pageSize, workerCount int) (map[string]interface{}, error) {
	config := make(map[string]interface{})

	query := `
		SELECT path, value, data_type 
		FROM caddy_config 
		WHERE enabled = true`
	// NOTE: Removing ALLOW FILTERING here. In production, adjust your schema to avoid filtering.

	caddy.Log().Named("adapters.cql").Info("Running Cassandra query", zap.String("query", query))
	iter := session.Query(query).PageSize(pageSize).Iter()

	// Channel to distribute rows to workers.
	rowCh := make(chan rowEntry, pageSize)
	var wg sync.WaitGroup
	var mu sync.Mutex

	// Launch worker pool.
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for row := range rowCh {
				// Debug logging at lower level can be enabled for troubleshooting.
				caddy.Log().Named("adapters.cql").Debug("Processing row",
					zap.String("path", row.path),
					zap.String("value", row.value),
					zap.String("data_type", row.dataType))
				parsedValue, err := parseValue(row.value, row.dataType)
				if err != nil {
					caddy.Log().Named("adapters.cql").Error("Error parsing value",
						zap.String("path", row.path),
						zap.String("value", row.value),
						zap.String("data_type", row.dataType),
						zap.Error(err))
					continue
				}
				pathParts := strings.Split(row.path, ".")
				// Lock the configuration while updating the nested value.
				mu.Lock()
				setNestedValue(config, pathParts, parsedValue)
				mu.Unlock()
			}
		}()
	}

	// Feed rows into the channel.
	var path, value, dataType string
	for iter.Scan(&path, &value, &dataType) {
		rowCh <- rowEntry{path: path, value: value, dataType: dataType}
	}
	close(rowCh)

	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("query iteration failed: %w", err)
	}

	wg.Wait()

	caddy.Log().Named("adapters.cql").Info("Successfully built configuration from Cassandra")
	return config, nil
}

// Adapt converts the Cassandra-stored configuration to Caddy's JSON format.
func (a Adapter) Adapt(body []byte, options map[string]interface{}) ([]byte, []caddyconfig.Warning, error) {
	var cfg CassandraAdapterConfig
	if err := json.Unmarshal(body, &cfg); err != nil {
		return nil, nil, fmt.Errorf("failed to unmarshal adapter configuration: %w", err)
	}
	if len(cfg.Hosts) == 0 || cfg.Keyspace == "" {
		return nil, nil, fmt.Errorf("contact_points and keyspace are required in the adapter configuration")
	}

	// Set default values if not provided.
	if cfg.PageSize <= 0 {
		cfg.PageSize = 1000
	}
	if cfg.WorkerCount <= 0 {
		cfg.WorkerCount = 8
	}

	caddy.Log().Named("adapters.cql").Info("Adapter configuration loaded", zap.Any("config", cfg))
	session, err := getSession(cfg)
	if err != nil {
		return nil, nil, err
	}
	defer session.Close()

	caddyConfig, err := getConfiguration(session, cfg.PageSize, cfg.WorkerCount)
	if err != nil {
		return nil, nil, err
	}

	jsonData, err := json.Marshal(caddyConfig)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to marshal final configuration to JSON: %w", err)
	}

	caddy.Log().Named("adapters.cql").Info("Final configuration JSON successfully generated")
	return jsonData, nil, nil
}

var _ caddyconfig.Adapter = (*Adapter)(nil)
