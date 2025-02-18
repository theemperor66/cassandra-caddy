package cassandraadapter

import (
	"encoding/json"
	"fmt"
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

type CassandraAdapterConfig struct {
	Hosts        []string `json:"contact_points"`
	Keyspace     string   `json:"keyspace"`
	QueryTimeout int      `json:"query_timeout"`
	LockTimeout  int      `json:"lock_timeout"`
	ConfigID     string   `json:"config_id"` // UUID string for the config to load
}

var session *gocql.Session

type Adapter struct{}

func getSession(config CassandraAdapterConfig) (*gocql.Session, error) {
	if session == nil {
		cluster := gocql.NewCluster(config.Hosts...)
		cluster.Keyspace = config.Keyspace
		cluster.Timeout = time.Duration(config.QueryTimeout) * time.Second

		var err error
		session, err = cluster.CreateSession()
		if err != nil {
			return nil, fmt.Errorf("failed to create session: %w", err)
		}

		// Create table if not exists
		createTableQuery := `
			CREATE TABLE IF NOT EXISTS caddy_config (
				config_id uuid,
				path text,
				value text,
				data_type text,
				enabled boolean,
				last_updated timestamp,
				PRIMARY KEY (config_id, path)
			) WITH CLUSTERING ORDER BY (path ASC)`

		if err := session.Query(createTableQuery).Exec(); err != nil {
			caddy.Log().Named("adapters.cql").Error(fmt.Sprintf("Create Table Error: %v", err))
			return nil, err
		}
	}
	return session, nil
}

// parseValue converts the string value to the appropriate Go type based on data_type
func parseValue(value string, dataType string) (interface{}, error) {
	switch dataType {
	case "string":
		// Remove any extra quotes that might be present
		return strings.Trim(value, "\""), nil
	case "number":
		var num float64
		if err := json.Unmarshal([]byte(value), &num); err != nil {
			// Try parsing the raw string if JSON unmarshal fails
			if n, err := json.Number(value).Float64(); err == nil {
				return n, nil
			}
			return nil, err
		}
		return num, nil
	case "boolean":
		var b bool
		if err := json.Unmarshal([]byte(value), &b); err != nil {
			// Try parsing the raw string if JSON unmarshal fails
			return value == "true", nil
		}
		return b, nil
	case "array", "object":
		var result interface{}
		// Handle potential double-escaped JSON
		unquoted, err := strconv.Unquote(value)
		if err == nil {
			value = unquoted
		}
		if err := json.Unmarshal([]byte(value), &result); err != nil {
			return nil, fmt.Errorf("failed to parse JSON value: %w", err)
		}
		return result, nil
	default:
		return nil, fmt.Errorf("unknown data type: %s", dataType)
	}
}

// setNestedValue recursively builds the configuration structure
func setNestedValue(config map[string]interface{}, path []string, value interface{}) {
	if len(path) == 1 {
		config[path[0]] = value
		return
	}

	key := path[0]
	if config[key] == nil {
		config[key] = make(map[string]interface{})
	}

	if m, ok := config[key].(map[string]interface{}); ok {
		setNestedValue(m, path[1:], value)
	}
}

func getConfiguration(configID gocql.UUID) (map[string]interface{}, error) {
	config := make(map[string]interface{})

	// Modified query to include enabled in the SELECT and scan
	iter := session.Query(`
		SELECT path, value, data_type 
		FROM caddy_config 
		WHERE config_id = ? AND enabled = true 
		ALLOW FILTERING`, configID).Iter()

	var entry ConfigEntry
	for iter.Scan(&entry.Path, &entry.Value, &entry.DataType) {
		// Parse the value according to its data type
		parsedValue, err := parseValue(entry.Value, entry.DataType)
		if err != nil {
			return nil, fmt.Errorf("error parsing value for path %s: %w", entry.Path, err)
		}

		// Split the path and build the nested structure
		pathParts := strings.Split(entry.Path, ".")

		// Handle array indices in path correctly
		for i, part := range pathParts {
			if strings.HasPrefix(part, "[") && strings.HasSuffix(part, "]") {
				// Convert array notation to regular path part
				pathParts[i] = strings.Trim(part, "[]")
			}
		}

		setNestedValue(config, pathParts, parsedValue)
	}

	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("error fetching config: %w", err)
	}

	return config, nil
}

func (a Adapter) Adapt(body []byte, options map[string]interface{}) ([]byte, []caddyconfig.Warning, error) {
	// Parse the adapter configuration
	config := CassandraAdapterConfig{
		QueryTimeout: 60,
		LockTimeout:  60,
	}

	if err := json.Unmarshal(body, &config); err != nil {
		return nil, nil, fmt.Errorf("failed to parse config: %w", err)
	}

	if len(config.Hosts) == 0 || config.Keyspace == "" {
		return nil, nil, fmt.Errorf("hosts and keyspace are required")
	}

	// Parse the config_id
	configID, err := gocql.ParseUUID(config.ConfigID)
	if err != nil {
		return nil, nil, fmt.Errorf("invalid config_id: %w", err)
	}

	// Get or create the session
	session, err = getSession(config)
	if err != nil {
		return nil, nil, err
	}

	// Get the configuration
	caddyConfig, err := getConfiguration(configID)
	if err != nil {
		return nil, nil, err
	}

	// Marshal the configuration to JSON
	jsonData, err := json.Marshal(caddyConfig)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to marshal config: %w", err)
	}

	return jsonData, nil, nil
}

var _ caddyconfig.Adapter = (*Adapter)(nil)
