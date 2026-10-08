/*
   Copyright 2014 Outbrain Inc.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package db

import (
	"crypto/tls"
	"fmt"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/patrickmn/go-cache"
	"github.com/proxysql/golib/log"
	"github.com/rcrowley/go-metrics"

	"github.com/proxysql/orchestrator/go/ssl"
)

const Error3159 = "Error 3159:"
const Error1045 = "Access denied for user"

var readInstanceTLSCounter = metrics.NewCounter()
var writeInstanceTLSCounter = metrics.NewCounter()
var readInstanceTLSCacheCounter = metrics.NewCounter()
var writeInstanceTLSCacheCounter = metrics.NewCounter()

func init() {
	_ = metrics.Register("instance_tls.read", readInstanceTLSCounter)
	_ = metrics.Register("instance_tls.write", writeInstanceTLSCounter)
	_ = metrics.Register("instance_tls.read_cache", readInstanceTLSCacheCounter)
	_ = metrics.Register("instance_tls.write_cache", writeInstanceTLSCacheCounter)
}

type SqlUtilsLogger struct {
	tenant             string // optional; empty for the legacy single-tenant backend
	client_context     string
	backend_connection bool
}

func (logger SqlUtilsLogger) OnError(caller_context string, query string, err error) error {
	query = strings.Join(strings.Fields(query), " ") // trim whitespaces
	query = strings.ReplaceAll(query, "%", "%%")     // escape %

	client_context := logger.client_context
	if logger.tenant != "" {
		client_context = logger.tenant + "/" + client_context
	}
	msg := fmt.Sprintf("%+v(%+v) %+v: %+v",
		caller_context,
		client_context,
		query,
		err)

	return log.Errorf("%s", msg)
}

// This validator is for dev purposes only. Call of this validator is
// disabled in sqlutils.go
var query_whitelist = []string{
	"substring_index(host, ':', 1) as slave_hostname",
}

func (logger SqlUtilsLogger) ValidateQuery(query string) {
	if logger.backend_connection {
		return
	}

	// check if whitelisted
	for i := 0; i < len(query_whitelist); i++ {
		if strings.Contains(query, query_whitelist[i]) {
			return
		}
	}

	lquery := strings.ToLower(query)
	if strings.Contains(lquery, "master") || strings.Contains(lquery, "slave") {
		_ = log.Error("QUERY CONTAINS MASTER / SLAVE: ")
		// panic("Query contains master/slave: " + query)
	}
}

func (b *Backend) tlsCache() *cache.Cache {
	b.requireTLSOnce.Do(func() {
		b.requireTLSCache = cache.New(time.Duration(b.cfg.TLSCacheTTLFactor*b.cfg.InstancePollSeconds)*time.Second, time.Second)
	})
	return b.requireTLSCache
}

// requiresTLS tells whether the topology server needs TLS, remembering the answer
// per backend (host:port and credentials are tenant specific).
func (b *Backend) requiresTLS(host string, port int, mysql_uri string) bool {
	cacheKey := fmt.Sprintf("%s:%d", host, port)
	tlsCache := b.tlsCache()

	if value, found := tlsCache.Get(cacheKey); found {
		readInstanceTLSCacheCounter.Inc(1)
		return value.(bool)
	}

	required := false
	db, err := b.topologyDB(mysql_uri, host, port)
	if err != nil {
		_ = log.Errore(err)
		return false
	}
	if err := db.Ping(); err != nil && (strings.Contains(err.Error(), Error3159) || strings.Contains(err.Error(), Error1045)) {
		required = true
	}

	query := `
			insert into
				database_instance_tls (
					hostname, port, required
				) values (
					?, ?, ?
				)
				on duplicate key update
					required=values(required)
				`
	if _, err := b.Exec(query, host, port, required); err != nil {
		_ = log.Errore(err)
	}
	writeInstanceTLSCounter.Inc(1)

	tlsCache.Set(cacheKey, required, cache.DefaultExpiration)
	writeInstanceTLSCacheCounter.Inc(1)

	return required
}

// topologyTLSName is the name under which this backend registers its topology TLS
// config with the mysql driver. The driver registry is process-wide, so the name
// must be unique per backend.
func (b *Backend) topologyTLSName() string {
	if b.legacyTLSNames {
		return "topology"
	}
	return "topology-" + b.id
}

// orchestratorTLSName: see topologyTLSName.
func (b *Backend) orchestratorTLSName() string {
	if b.legacyTLSNames {
		return "orchestrator"
	}
	return "orchestrator-" + b.id
}

// redactDSN returns the DSN without its password, safe to put in errors and
// logs. A DSN that cannot be parsed is not echoed at all.
func redactDSN(dsn string) string {
	if safe, err := matchDSN(dsn); err == nil {
		return safe
	}
	return "<dsn redacted>"
}

// setupTopologyTLS creates a TLS configuration from the config supplied CA, Certificate, and Private key.
// It registers it with the mysql driver under topologyTLSName and modifies the supplied URI to use it.
func (b *Backend) setupTopologyTLS(uri string) (string, error) {
	b.tlsMu.Lock()
	defer b.tlsMu.Unlock()
	name := b.topologyTLSName()
	if !b.topologyTLSConfigured {
		cfg := b.cfg
		tlsConfig, err := ssl.NewTLSConfig(cfg.MySQLTopologySSLCAFile, !cfg.MySQLTopologySSLSkipVerify)
		if err != nil {
			return "", log.Errorf("Can't create TLS configuration for Topology connection %s: %s", redactDSN(uri), err)
		}
		// Drop to TLS 1.0 for talking to MySQL
		tlsConfig.MinVersion = tls.VersionTLS10
		tlsConfig.InsecureSkipVerify = cfg.MySQLTopologySSLSkipVerify

		if (cfg.MySQLTopologyUseMutualTLS && !cfg.MySQLTopologySSLSkipVerify) &&
			cfg.MySQLTopologySSLCertFile != "" &&
			cfg.MySQLTopologySSLPrivateKeyFile != "" {
			if err = ssl.AppendKeyPair(tlsConfig, cfg.MySQLTopologySSLCertFile, cfg.MySQLTopologySSLPrivateKeyFile); err != nil {
				return "", log.Errorf("Can't setup TLS key pairs for %s: %s", redactDSN(uri), err)
			}
		}
		if err = mysql.RegisterTLSConfig(name, tlsConfig); err != nil {
			return "", log.Errorf("Can't register mysql TLS config for topology: %s", err)
		}
		b.topologyTLSConfigured = true
	}
	return fmt.Sprintf("%s&tls=%s", uri, name), nil
}

// setupOrchestratorTLS is setupTopologyTLS for the backend connection.
func (b *Backend) setupOrchestratorTLS(uri string) (string, error) {
	b.tlsMu.Lock()
	defer b.tlsMu.Unlock()
	name := b.orchestratorTLSName()
	if !b.orchestratorTLSConfigured {
		cfg := b.cfg
		tlsConfig, err := ssl.NewTLSConfig(cfg.MySQLOrchestratorSSLCAFile, !cfg.MySQLOrchestratorSSLSkipVerify)
		if err != nil {
			return "", fmt.Errorf("can't create TLS configuration for Orchestrator connection %s: %w", redactDSN(uri), err)
		}
		// Drop to TLS 1.0 for talking to MySQL
		tlsConfig.MinVersion = tls.VersionTLS10
		tlsConfig.InsecureSkipVerify = cfg.MySQLOrchestratorSSLSkipVerify
		if (!cfg.MySQLOrchestratorSSLSkipVerify) &&
			cfg.MySQLOrchestratorSSLCertFile != "" &&
			cfg.MySQLOrchestratorSSLPrivateKeyFile != "" {
			if err = ssl.AppendKeyPair(tlsConfig, cfg.MySQLOrchestratorSSLCertFile, cfg.MySQLOrchestratorSSLPrivateKeyFile); err != nil {
				return "", fmt.Errorf("can't setup TLS key pairs for %s: %w", redactDSN(uri), err)
			}
		}
		if err = mysql.RegisterTLSConfig(name, tlsConfig); err != nil {
			return "", fmt.Errorf("can't register mysql TLS config for orchestrator: %w", err)
		}
		b.orchestratorTLSConfigured = true
	}
	return fmt.Sprintf("%s&tls=%s", uri, name), nil
}

// SetupMySQLTopologyTLS is the legacy single-tenant entry point (default backend).
func SetupMySQLTopologyTLS(uri string) (string, error) {
	return DefaultBackend().setupTopologyTLS(uri)
}

// SetupMySQLOrchestratorTLS is the legacy single-tenant entry point (default backend).
// As it always did, it exits the process when the TLS setup fails.
func SetupMySQLOrchestratorTLS(uri string) (string, error) {
	uri, err := DefaultBackend().setupOrchestratorTLS(uri)
	if err != nil {
		return "", log.Fatale(err)
	}
	return uri, nil
}
