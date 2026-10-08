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
	"database/sql"
	"errors"
	"fmt"
	"regexp"

	"github.com/proxysql/golib/sqlutils"
	"github.com/proxysql/orchestrator/go/config"
)

const dsnRegexp = `^([^:]+)(:.*)?(@(socket|tcp)\(.+\)/.*)$`

var (
	EmptyArgs  []interface{}
	ErrNoMatch = errors.New("no match")
)

type DummySqlResult struct {
}

func (this DummySqlResult) LastInsertId() (int64, error) {
	return 0, nil
}

func (this DummySqlResult) RowsAffected() (int64, error) {
	return 1, nil
}

// The package-level functions below are the legacy single-tenant API. They are
// thin shims over DefaultBackend() and behave exactly as they always did; new
// code should hold a *Backend instead.

// getMySQLURI returns the DSN of the default backend's MySQL database.
func getMySQLURI() string {
	uri, _ := DefaultBackend().mysqlDSN()
	return uri
}

// OpenDiscovery returns a DB instance to access a topology instance.
// It has lower read timeout than OpenTopology and is intended to
// be used with low-latency discovery queries.
func OpenDiscovery(host string, port int) (*sql.DB, error) {
	return DefaultBackend().OpenDiscovery(host, port)
}

// OpenTopology returns a DB instance to access a topology instance.
func OpenTopology(host string, port int) (*sql.DB, error) {
	return DefaultBackend().OpenTopology(host, port)
}

func IsSQLite() bool {
	return DefaultBackend().IsSQLite()
}

// matchDSN tries to match the DSN or returns an error
func matchDSN(dsn string) (string, error) {
	re := regexp.MustCompile(dsnRegexp)

	matches := re.FindStringSubmatch(dsn)
	if matches == nil {
		return "", ErrNoMatch
	}

	// matching dsn so printout excluding the password
	// - if no password is provided the lack of a password will be visible!
	maskedPass := ""
	if len(matches[2]) > 0 {
		maskedPass = `:?`
	}

	return matches[1] + maskedPass + matches[3], nil
}

// safeMySQLURI returns a version of the dsn without showing the password so it can be logged safely.
// - if we are unable to correctly match the provided dsn we build an incomplete one based on some of the settings.
func safeMySQLURI(dsn string) string {
	return safeMySQLURIFor(config.Config, dsn)
}

func safeMySQLURIFor(cfg *config.Configuration, dsn string) string {
	if safeDSN, err := matchDSN(dsn); err == nil {
		return safeDSN
	}

	// Fallback to use an incomplete dsn which may be good enough.
	// Do not show the password but do show what we connect to.
	return fmt.Sprintf("%s:?@tcp(%s:%d)/%s?timeout=%ds (incomplete dsn!)",
		cfg.MySQLOrchestratorUser,
		cfg.MySQLOrchestratorHost,
		cfg.MySQLOrchestratorPort,
		cfg.MySQLOrchestratorDatabase,
		cfg.MySQLConnectTimeoutSeconds)
}

// OpenOrchestrator returns the DB instance for the orchestrator backed database
func OpenOrchestrator() (db *sql.DB, err error) {
	return DefaultBackend().Open()
}

// ExecOrchestrator will execute given query on the orchestrator backend database.
func ExecOrchestrator(query string, args ...interface{}) (sql.Result, error) {
	return DefaultBackend().Exec(query, args...)
}

// QueryRowsMapOrchestrator
func QueryOrchestratorRowsMap(query string, on_row func(sqlutils.RowMap) error) error {
	return DefaultBackend().QueryRowsMap(query, on_row)
}

// QueryOrchestrator
func QueryOrchestrator(query string, argsArray []interface{}, on_row func(sqlutils.RowMap) error) error {
	return DefaultBackend().Query(query, argsArray, on_row)
}

// QueryOrchestratorRowsMapBuffered
func QueryOrchestratorRowsMapBuffered(query string, on_row func(sqlutils.RowMap) error) error {
	return DefaultBackend().QueryRowsMapBuffered(query, on_row)
}

// QueryOrchestratorBuffered
func QueryOrchestratorBuffered(query string, argsArray []interface{}, on_row func(sqlutils.RowMap) error) error {
	return DefaultBackend().QueryBuffered(query, argsArray, on_row)
}

// ReadTimeNow reads and returns the current timestamp as string. See Backend.ReadTimeNow.
func ReadTimeNow() (timeNow string, err error) {
	return DefaultBackend().ReadTimeNow()
}
