package cassandracfg

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig"
	"github.com/gocql/gocql"
)

func init() {
	caddyconfig.RegisterAdapter("cassandra", Adapter{})
}

type CassandraAdapterConfig struct {
	Hosts           []string `json:"hosts"`
	Keyspace        string   `json:"keyspace"`
	TableNamePrefix string   `json:"tableNamePrefix"`
	RefreshInterval int64    `json:"refreshInterval"`
	Username        string   `json:"username,omitempty"`
	Password        string   `json:"password,omitempty"`
}

var session *gocql.Session
var tableName string
var config_version = "0"

type Adapter struct{}

func getSession(cassandraConfig CassandraAdapterConfig) (*gocql.Session, error) {
	if session == nil {
		cluster := gocql.NewCluster(cassandraConfig.Hosts...)
		cluster.Keyspace = cassandraConfig.Keyspace
		if cassandraConfig.Username != "" && cassandraConfig.Password != "" {
			cluster.Authenticator = gocql.PasswordAuthenticator{
				Username: cassandraConfig.Username,
				Password: cassandraConfig.Password,
			}
		}

		var err error
		session, err = cluster.CreateSession()
		if err != nil {
			return nil, err
		}

		tableName = cassandraConfig.TableNamePrefix + "_CONFIG"

		// Create table if not exists
		createTableQuery := fmt.Sprintf(`
			CREATE TABLE IF NOT EXISTS %s (
				id timeuuid,
				key text,
				value text,
				enable boolean,
				created timestamp,
				PRIMARY KEY (key, created)
			) WITH CLUSTERING ORDER BY (created DESC)
		`, tableName)

		if err := session.Query(createTableQuery).Exec(); err != nil {
			caddy.Log().Named("adapters.cassandra.config").Error(fmt.Sprintf("Create Table Error: %v", err))
			return nil, err
		}
	}
	return session, nil
}

func getValueFromDb(key string) (string, error) {
	var value string
	err := session.Query(`SELECT value FROM `+tableName+` WHERE key = ? AND enable = true LIMIT 1`, key).Scan(&value)
	if err == gocql.ErrNotFound {
		return "", nil
	}
	return value, err
}

func getValuesFromDb(key string) ([]string, error) {
	var values []string
	iter := session.Query(`SELECT value FROM `+tableName+` WHERE key = ? AND enable = true`, key).Iter()
	var value string
	for iter.Scan(&value) {
		values = append(values, value)
	}
	return values, iter.Close()
}

func getConfiguration() ([]byte, error) {
	var value string
	var err error

	value, err = getValueFromDb("config")
	if err != nil {
		return nil, err
	}

	config := caddy.Config{}

	if value != "" {
		err = json.Unmarshal([]byte(value), &config)
		if err != nil {
			return nil, err
		}
	}

	// Load admin config
	if value, err = getValueFromDb("config.admin"); err == nil && value != "" {
		if config.Admin == nil {
			configAdmin := &caddy.AdminConfig{}
			err = json.Unmarshal([]byte(value), configAdmin)
			config.Admin = configAdmin
		} else {
			err = json.Unmarshal([]byte(value), config.Admin)
		}
	}

	// Similar pattern for other config sections...
	// Load apps config
	if value, err = getValueFromDb("config.apps"); err == nil && value != "" {
		if config.AppsRaw == nil {
			configAppsRaw := caddy.ModuleMap{}
			err = json.Unmarshal([]byte(value), &configAppsRaw)
			config.AppsRaw = configAppsRaw
		} else {
			err = json.Unmarshal([]byte(value), &config.AppsRaw)
		}
	}

	return json.Marshal(config)
}

func (a Adapter) Adapt(body []byte, options map[string]interface{}) ([]byte, []caddyconfig.Warning, error) {
	cassandraConfig := CassandraAdapterConfig{
		TableNamePrefix: "CADDY",
		RefreshInterval: 100,
	}

	err := json.Unmarshal(body, &cassandraConfig)
	if err != nil {
		return nil, nil, err
	}

	if len(cassandraConfig.Hosts) == 0 || cassandraConfig.Keyspace == "" {
		caddy.Log().Named("adapters.cassandra.config").Error("Hosts or Keyspace not found")
		panic("CassandraAdapter configuration incomplete")
	}

	session, err = getSession(cassandraConfig)
	if err != nil {
		return nil, nil, err
	}

	config, err := getConfiguration()
	if err != nil {
		return nil, nil, err
	}

	config_version_new := getConfigVersion()
	config_version = config_version_new

	runCheckLoop(cassandraConfig)
	return config, nil, err
}

// Helper functions for version checking and config refresh
func getConfigVersion() string {
	var version string
	err := session.Query(`SELECT value FROM `+tableName+` WHERE key = ? AND enable = true LIMIT 1`, "version").Scan(&version)
	if err != nil {
		caddy.Log().Named("adapters.cassandra.checkloop").Error(fmt.Sprintf("Error getting config version: %v", err))
		return config_version
	}
	return version
}

func refreshConfig(config_version_new string) {
	config, err := getConfiguration()
	if err != nil {
		caddy.Log().Named("adapters.cassandra.refreshConfig").Debug(fmt.Sprintf("err %v", err))
		return
	}
	config_version = config_version_new
	caddy.Load(config, false)
}

func checkAndRefreshConfig(cassandraConfig CassandraAdapterConfig) {
	config_version_new := getConfigVersion()
	if config_version_new != config_version {
		refreshConfig(config_version_new)
	}
}

func runCheckLoop(cassandraConfig CassandraAdapterConfig) {
	done := make(chan bool)
	go func(t time.Duration) {
		tick := time.NewTicker(t).C
		for {
			select {
			case <-tick:
				checkAndRefreshConfig(cassandraConfig)
			case <-done:
				return
			}
		}
	}(time.Second * time.Duration(cassandraConfig.RefreshInterval))
}

var _ caddyconfig.Adapter = (*Adapter)(nil)
