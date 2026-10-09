/*
   Copyright 2026 Orchestrator Authors

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

package logic

import (
	"database/sql"
	"fmt"
	"hash/crc32"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/proxysql/orchestrator/go/config"
)

// ORCH_TEST_MYSQL_DSN ("user:password@tcp(host:3306)/", a user allowed to create databases and to have all
// privileges on `orc\_%`) turns the tenant test helpers of this package (testBase, testTenantConfigs and so
// newTestManager) from SQLite backends into MySQL/MariaDB ones: one database per tenant plus one for the control
// backend, all on that one server, dropped when the test ends. The WHOLE test suite of the package then runs
// against a real MySQL backend:
//
//	ORCH_TEST_MYSQL_DSN='orcbk:orcbk-secret@tcp(172.31.90.200:3306)/' go test ./go/logic/
//
// Without the variable nothing changes (SQLite, no server needed).
const mysqlTestDSNEnv = "ORCH_TEST_MYSQL_DSN"

var mysqlTestSeq atomic.Uint32

// mysqlTestServer returns the connection settings of the test server, or nil when the variable is not set.
func mysqlTestServer(t *testing.T) *mysql.Config {
	t.Helper()
	dsn := os.Getenv(mysqlTestDSNEnv)
	if dsn == "" {
		return nil
	}
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		t.Fatalf("%s: %v", mysqlTestDSNEnv, err)
	}
	return cfg
}

// mysqlTestDatabase returns a fresh database name for the given role (a tenant id or "control") and drops it at cleanup.
func mysqlTestDatabase(t *testing.T, srv *mysql.Config, role string) string {
	t.Helper()
	name := fmt.Sprintf("orc_t%x_%d_%s", crc32.ChecksumIEEE([]byte(t.Name())), mysqlTestSeq.Add(1), role)
	if len(name) > 64 {
		name = fmt.Sprintf("orc_t%x_%d_%x", crc32.ChecksumIEEE([]byte(t.Name())), mysqlTestSeq.Add(1), crc32.ChecksumIEEE([]byte(role)))
	}
	t.Cleanup(func() {
		db, err := sql.Open("mysql", srv.FormatDSN())
		if err != nil {
			return
		}
		defer db.Close()
		_, _ = db.Exec("drop database if exists `" + name + "`")
	})
	return name
}

// useMySQLBackend points the backend settings of cfg at database db of the test server.
func useMySQLBackend(cfg *config.Configuration, srv *mysql.Config, db string) {
	host, port := srv.Addr, uint(3306)
	if h, p, err := net.SplitHostPort(srv.Addr); err == nil {
		if n, err := strconv.ParseUint(p, 10, 32); err == nil {
			host, port = h, uint(n)
		}
	}
	cfg.BackendDB = "mysql"
	cfg.MySQLOrchestratorHost = host
	cfg.MySQLOrchestratorPort = port
	cfg.MySQLOrchestratorUser = srv.User
	cfg.MySQLOrchestratorPassword = srv.Passwd
	cfg.MySQLOrchestratorDatabase = db
}

// parseBackendTime parses a time column as the backends hand it over: RFC3339 from the SQLite driver, the plain
// "2006-01-02 15:04:05" of the MySQL one (UTC, like the containers the MySQL tests run in).
func parseBackendTime(s string) (time.Time, error) {
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t, nil
	}
	return time.Parse("2006-01-02 15:04:05", s)
}

// breakBackend makes the backend settings of cfg unusable (the tenant must then fail to initialise).
func breakBackend(t *testing.T, cfg *config.Configuration) {
	t.Helper()
	if cfg.IsMySQL() {
		cfg.MySQLOrchestratorPassword = "not-the-password"
		return
	}
	cfg.SQLite3DataFile = filepath.Join(t.TempDir(), "no-such-dir", "deeper", "bad.sqlite3")
}

// unsetBackend removes what names the backend in cfg (the database or the SQLite file).
func unsetBackend(cfg *config.Configuration) {
	if cfg.IsMySQL() {
		cfg.MySQLOrchestratorDatabase = ""
		return
	}
	cfg.SQLite3DataFile = ""
}

// skipOnMySQL skips a test that is about a SQLite behaviour (when the helpers run on MySQL, see ORCH_TEST_MYSQL_DSN).
func skipOnMySQL(t *testing.T, why string) {
	t.Helper()
	if os.Getenv(mysqlTestDSNEnv) != "" {
		t.Skip(why)
	}
}
