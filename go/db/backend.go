package db

import (
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/patrickmn/go-cache"
	"github.com/proxysql/golib/log"
	"github.com/proxysql/golib/sqlutils"
	"github.com/proxysql/orchestrator/go/config"
)

// DefaultBackendID is the tenant id of the legacy single-tenant backend.
const DefaultBackendID = "default"

// ErrBackendClosed is returned by a Backend that has been Close()d.
var ErrBackendClosed = errors.New("db: backend is closed")

// ErrDefaultBackendDisabled is returned by the Backend that DefaultBackend()
// hands out in a multi-tenant process (see DisableDefaultBackend).
var ErrDefaultBackendDisabled = errors.New("db: the legacy default backend is disabled in a multi-tenant process; use the backend of the tenant")

// Backend is the orchestrator backend database (SQLite or MySQL) of ONE tenant,
// plus the connections to the topology servers that tenant monitors.
//
// A Backend owns everything that used to be package-level state: its *sql.DB
// handle (not shared through sqlutils' global cache), its SQLite dialect, its
// "schema ready" state, the TLS configs registered with the mysql driver
// (names are unique per backend) and the "requires TLS" cache. The cfg passed to
// NewBackend must not be mutated afterwards.
//
// Locking: a Backend never takes a process-wide lock per query. The handle is
// resolved once (NewBackend / first use) and published through an atomic
// pointer, so Exec/Query contend only inside database/sql, which is safe for
// concurrent use. A SQLite backend serialises through MaxOpenConns(1) on its own
// file, so N tenants have N independent serialisers.
type Backend struct {
	id  string
	cfg *config.Configuration

	// live: cfg is the (mutable) global config.Config and the handle is
	// re-resolved from it on every call, exactly like the legacy package-level
	// functions did. Only the DefaultBackend is live.
	live bool
	// autoSchema: EnsureSchema runs lazily on first connect (legacy behaviour).
	autoSchema bool
	// fatalOnSchemaError: legacy behaviour, a schema failure exits the process.
	fatalOnSchemaError bool
	// legacyTLSNames: register the TLS configs as "topology"/"orchestrator"
	// instead of "topology-<id>"/"orchestrator-<id>".
	legacyTLSNames bool
	// disabled: the stand-in DefaultBackend() returns in a multi-tenant process.
	// It never touches the configuration or a database: every operation fails
	// with ErrDefaultBackendDisabled.
	disabled bool

	closed atomic.Bool

	connectMu sync.Mutex // serialises connect()
	fixed     atomic.Pointer[backendConn]
	connsMu   sync.RWMutex
	conns     map[string]*backendConn // live backends only, by handle key

	topoMu  sync.RWMutex
	topoDBs map[string]*sql.DB

	tlsMu                     sync.Mutex
	topologyTLSConfigured     bool
	orchestratorTLSConfigured bool

	requireTLSOnce  sync.Once
	requireTLSCache *cache.Cache
}

type backendConn struct {
	db         *sql.DB
	schemaMu   sync.Mutex
	schemaDone bool
}

// NewBackend builds the backend of tenant id from cfg. It validates the backend
// settings and, for SQLite, opens (and thereby creates) the data file, so an
// unusable path fails here. For MySQL no network access happens until first use.
// It never exits the process: every failure is returned. The schema is NOT
// deployed; call EnsureSchema.
func NewBackend(id string, cfg *config.Configuration) (*Backend, error) {
	if id == "" {
		return nil, errors.New("db: backend id must not be empty")
	}
	if cfg == nil {
		return nil, fmt.Errorf("db: backend %q: nil configuration", id)
	}
	switch {
	case cfg.IsSQLite():
		if cfg.SQLite3DataFile == "" {
			return nil, fmt.Errorf("db: backend %q: SQLite3DataFile must be set when BackendDB is %q", id, cfg.BackendDB)
		}
	case cfg.IsMySQL():
	default:
		return nil, fmt.Errorf("db: backend %q: unsupported BackendDB %q", id, cfg.BackendDB)
	}
	b := newBackend(id, cfg)
	if cfg.IsSQLite() {
		if _, err := b.connection(); err != nil {
			return nil, fmt.Errorf("db: backend %q: %w", id, err)
		}
	}
	return b, nil
}

func newBackend(id string, cfg *config.Configuration) *Backend {
	return &Backend{
		id:      id,
		cfg:     cfg,
		conns:   make(map[string]*backendConn),
		topoDBs: make(map[string]*sql.DB),
	}
}

var (
	defaultBackend     *Backend
	defaultBackendOnce sync.Once

	defaultBackendDisabled atomic.Bool
	disabledBackend        = sync.OnceValue(func() *Backend {
		b := newBackend(DefaultBackendID, config.Config)
		b.disabled = true
		return b
	})
)

// DisableDefaultBackend makes DefaultBackend() return a Backend whose every
// operation fails with ErrDefaultBackendDisabled, instead of the legacy backend
// driven by the global config.Config. The TenantManager calls it when the
// process runs from a tenants file: there the base configuration describes no
// tenant, so code that still reaches the legacy backend (a call that was not
// migrated to its tenant's Backend) must fail with an error -- it would
// otherwise create and migrate a schema on the base backend, and exit the
// process if that fails. In the legacy single-tenant setup nothing changes.
func DisableDefaultBackend() { defaultBackendDisabled.Store(true) }

// DisabledBackend returns the inert Backend that DefaultBackend() hands out once
// disabled: every operation fails with ErrDefaultBackendDisabled.
func DisabledBackend() *Backend { return disabledBackend() }

// EnableDefaultBackend undoes DisableDefaultBackend (the TenantManager that
// disabled it was stopped).
func EnableDefaultBackend() { defaultBackendDisabled.Store(false) }

// DefaultBackend returns the legacy single-tenant backend, driven by the global
// config.Config. It is built lazily (nothing is read from the config until the
// first query, so it is safe to call before the config is loaded) and is live:
// changes to config.Config are honoured, as with the former package functions.
// A schema failure on this backend exits the process, as it always did.
// After DisableDefaultBackend it returns an inert Backend that only returns
// ErrDefaultBackendDisabled.
func DefaultBackend() *Backend {
	if defaultBackendDisabled.Load() {
		return disabledBackend()
	}
	defaultBackendOnce.Do(func() {
		b := newBackend(DefaultBackendID, config.Config)
		b.live = true
		b.autoSchema = true
		b.fatalOnSchemaError = true
		b.legacyTLSNames = true
		defaultBackend = b
	})
	return defaultBackend
}

// ID returns the tenant id of this backend.
func (b *Backend) ID() string { return b.id }

// Config returns the configuration the backend was built from. Read-only.
func (b *Backend) Config() *config.Configuration { return b.cfg }

// IsSQLite tells whether this backend speaks the SQLite dialect.
func (b *Backend) IsSQLite() bool { return b.cfg.IsSQLite() }

// translateStatement adapts a statement to this backend's SQL dialect.
func (b *Backend) translateStatement(statement string) string {
	if b.cfg.IsSQLite() {
		return sqlutils.ToSqlite3Dialect(statement)
	}
	return statement
}

func (b *Backend) tenantLogContext() string {
	if b.legacyTLSNames {
		return ""
	}
	return b.id
}

func (b *Backend) backendLogger() SqlUtilsLogger {
	cfg := b.cfg
	if cfg.IsSQLite() {
		return SqlUtilsLogger{tenant: b.tenantLogContext(), client_context: cfg.SQLite3DataFile, backend_connection: true}
	}
	return SqlUtilsLogger{
		tenant:             b.tenantLogContext(),
		client_context:     cfg.MySQLOrchestratorHost + ":" + strconv.FormatUint(uint64(cfg.MySQLOrchestratorPort), 10),
		backend_connection: true,
	}
}

// Open returns the backend's *sql.DB (e.g. to run a transaction). The handle is
// owned by the Backend: callers must not close it.
func (b *Backend) Open() (*sql.DB, error) {
	c, err := b.connection()
	if err != nil {
		return nil, err
	}
	return c.db, nil
}

func (b *Backend) connection() (*backendConn, error) {
	if b.disabled {
		return nil, ErrDefaultBackendDisabled
	}
	if b.closed.Load() {
		return nil, ErrBackendClosed
	}
	if !b.live {
		if c := b.fixed.Load(); c != nil {
			return c, nil
		}
		return b.connect("")
	}
	key, err := b.liveKey()
	if err != nil {
		return nil, err
	}
	b.connsMu.RLock()
	c := b.conns[key]
	b.connsMu.RUnlock()
	if c != nil {
		return c, nil
	}
	return b.connect(key)
}

// liveKey identifies the handle the global config currently points to.
func (b *Backend) liveKey() (string, error) {
	if b.cfg.IsSQLite() {
		return b.cfg.SQLite3DataFile, nil
	}
	return b.mysqlDSN()
}

func (b *Backend) lookup(key string) *backendConn {
	if !b.live {
		return b.fixed.Load()
	}
	b.connsMu.RLock()
	defer b.connsMu.RUnlock()
	return b.conns[key]
}

func (b *Backend) publish(key string, c *backendConn) {
	if !b.live {
		b.fixed.Store(c)
		return
	}
	b.connsMu.Lock()
	b.conns[key] = c
	b.connsMu.Unlock()
}

func (b *Backend) connect(key string) (*backendConn, error) {
	b.connectMu.Lock()
	defer b.connectMu.Unlock()
	if b.closed.Load() {
		return nil, ErrBackendClosed
	}
	if c := b.lookup(key); c != nil {
		return c, nil
	}
	cfg := b.cfg

	var db *sql.DB
	var err error
	if cfg.IsSQLite() {
		if err = createSQLiteFile(cfg.SQLite3DataFile); err != nil {
			return nil, fmt.Errorf("cannot create sqlite backend %q: %w", cfg.SQLite3DataFile, err)
		}
		if db, err = sqlutils.NewDB("sqlite3", cfg.SQLite3DataFile, nil); err != nil {
			return nil, err
		}
		if err = db.Ping(); err != nil {
			_ = sqlutils.CloseDB(db)
			return nil, fmt.Errorf("cannot open sqlite backend %q: %w", cfg.SQLite3DataFile, err)
		}
		log.Debugf("Connected to orchestrator backend: sqlite on %v", cfg.SQLite3DataFile)
		db.SetMaxOpenConns(1)
		db.SetMaxIdleConns(1)
	} else {
		if err = b.createMySQLDatabase(); err != nil {
			return nil, log.Errore(err)
		}
		dsn, derr := b.mysqlDSN()
		if derr != nil {
			return nil, log.Errore(derr)
		}
		if db, err = sqlutils.NewDB("mysql", dsn, b.backendLogger()); err != nil {
			return nil, err
		}
		log.Debugf("Connected to orchestrator backend: %v", safeMySQLURIFor(cfg, dsn))
		if cfg.MySQLOrchestratorMaxPoolConnections > 0 {
			log.Debugf("Orchestrator pool SetMaxOpenConns: %d", cfg.MySQLOrchestratorMaxPoolConnections)
			db.SetMaxOpenConns(cfg.MySQLOrchestratorMaxPoolConnections)
		}
		if cfg.MySQLConnectionLifetimeSeconds > 0 {
			db.SetConnMaxLifetime(time.Duration(cfg.MySQLConnectionLifetimeSeconds) * time.Second)
		}
	}
	c := &backendConn{db: db}
	if b.autoSchema {
		if err := b.ensureSchema(c); err != nil {
			if b.fatalOnSchemaError {
				_ = log.Fatale(err)
			}
			_ = sqlutils.CloseDB(db)
			return nil, err
		}
	}
	// A low value here will trigger reconnects which could
	// make the number of backend connections hit the tcp
	// limit. That's bad.  I could make this setting dynamic
	// but then people need to know which value to use. For now
	// allow up to 25% of MySQLOrchestratorMaxPoolConnections
	// to be idle.  That should provide a good number which
	// does not keep the maximum number of connections open but
	// at the same time does not trigger disconnections and
	// reconnections too frequently.
	maxIdleConns := int(cfg.MySQLOrchestratorMaxPoolConnections * 25 / 100)
	if maxIdleConns < 10 {
		maxIdleConns = 10
	}
	log.Infof("Connecting to backend %s:%d: maxConnections: %d, maxIdleConns: %d",
		cfg.MySQLOrchestratorHost,
		cfg.MySQLOrchestratorPort,
		cfg.MySQLOrchestratorMaxPoolConnections,
		maxIdleConns)
	if !b.legacyTLSNames {
		log.Infof("Backend of tenant %s is ready", b.id)
	}
	db.SetMaxIdleConns(maxIdleConns)
	b.publish(key, c)
	return c, nil
}

// createSQLiteFile creates the data file of a SQLite backend that does not exist
// yet, readable by its owner only: it holds host names, aliases and access
// tokens, and SQLite would otherwise create it with the umask's 0644. The WAL
// and shared-memory files SQLite adds next to it inherit its permissions. An
// existing file is left alone (a too permissive one is only reported). The
// directory is not created: a missing one is an error, as before.
func createSQLiteFile(path string) error {
	if path == "" || path == ":memory:" || strings.HasPrefix(path, "file:") {
		return nil
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err == nil {
		return f.Close()
	}
	if !errors.Is(err, fs.ErrExist) {
		return err
	}
	if info, statErr := os.Stat(path); statErr == nil && info.Mode().Perm()&0o077 != 0 {
		log.Warningf("sqlite backend %q is accessible by users other than its owner (mode %v); it holds access tokens: consider chmod 600", path, info.Mode().Perm())
	}
	return nil
}

// createMySQLDatabase issues "create database if not exists" on a short-lived
// connection that is not tied to a database.
func (b *Backend) createMySQLDatabase() error {
	cfg := b.cfg
	uri, err := b.mysqlGenericDSN()
	if err != nil {
		return err
	}
	db, err := sqlutils.NewDB("mysql", uri, b.backendLogger())
	if err != nil {
		return err
	}
	defer sqlutils.CloseDB(db)
	_, err = db.Exec(fmt.Sprintf("create database if not exists %s", cfg.MySQLOrchestratorDatabase))
	return err
}

func (b *Backend) mysqlGenericDSN() (string, error) {
	if b.disabled {
		return "", ErrDefaultBackendDisabled
	}
	cfg := b.cfg
	uri := fmt.Sprintf("%s:%s@tcp(%s:%d)/?timeout=%ds&readTimeout=%ds&interpolateParams=true",
		cfg.MySQLOrchestratorUser,
		cfg.MySQLOrchestratorPassword,
		cfg.MySQLOrchestratorHost,
		cfg.MySQLOrchestratorPort,
		cfg.MySQLConnectTimeoutSeconds,
		cfg.MySQLOrchestratorReadTimeoutSeconds,
	)
	if cfg.MySQLOrchestratorUseMutualTLS {
		return b.setupOrchestratorTLS(uri)
	}
	return uri, nil
}

func (b *Backend) mysqlDSN() (string, error) {
	if b.disabled {
		return "", ErrDefaultBackendDisabled
	}
	cfg := b.cfg
	uri := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?timeout=%ds&readTimeout=%ds&rejectReadOnly=%t&interpolateParams=true",
		cfg.MySQLOrchestratorUser,
		cfg.MySQLOrchestratorPassword,
		cfg.MySQLOrchestratorHost,
		cfg.MySQLOrchestratorPort,
		cfg.MySQLOrchestratorDatabase,
		cfg.MySQLConnectTimeoutSeconds,
		cfg.MySQLOrchestratorReadTimeoutSeconds,
		cfg.MySQLOrchestratorRejectReadOnly,
	)
	if cfg.MySQLOrchestratorUseMutualTLS {
		var err error
		if uri, err = b.setupOrchestratorTLS(uri); err != nil {
			return "", err
		}
	}
	if cfg.MySQLOrchestratorMaxAllowedPacket >= 0 {
		uri = fmt.Sprintf("%s&maxAllowedPacket=%d", uri, cfg.MySQLOrchestratorMaxAllowedPacket)
	}
	return uri, nil
}

// EnsureSchema creates or upgrades the backend schema and records the deployed
// version in orchestrator_db_deployments. It is idempotent per Backend and
// returns errors instead of exiting. It does nothing when the configuration says
// SkipOrchestratorDatabaseUpdate.
func (b *Backend) EnsureSchema() error {
	c, err := b.connection()
	if err != nil {
		return err
	}
	return b.ensureSchema(c)
}

func (b *Backend) ensureSchema(c *backendConn) error {
	c.schemaMu.Lock()
	defer c.schemaMu.Unlock()
	if c.schemaDone || b.cfg.SkipOrchestratorDatabaseUpdate {
		return nil
	}
	if err := b.initOrchestratorDB(c.db); err != nil {
		return err
	}
	c.schemaDone = true
	return nil
}

// Close releases the backend handle and its topology connection pools. A closed
// Backend returns ErrBackendClosed. It is safe to call more than once.
func (b *Backend) Close() error {
	if !b.closed.CompareAndSwap(false, true) {
		return nil
	}
	b.connectMu.Lock()
	defer b.connectMu.Unlock()
	var errs []error
	if c := b.fixed.Swap(nil); c != nil {
		errs = append(errs, sqlutils.CloseDB(c.db))
	}
	b.connsMu.Lock()
	for k, c := range b.conns {
		errs = append(errs, sqlutils.CloseDB(c.db))
		delete(b.conns, k)
	}
	b.connsMu.Unlock()
	b.topoMu.Lock()
	for k, db := range b.topoDBs {
		errs = append(errs, sqlutils.CloseDB(db))
		delete(b.topoDBs, k)
	}
	b.topoMu.Unlock()
	b.tlsMu.Lock()
	if b.topologyTLSConfigured {
		mysql.DeregisterTLSConfig(b.topologyTLSName())
		b.topologyTLSConfigured = false
	}
	if b.orchestratorTLSConfigured {
		mysql.DeregisterTLSConfig(b.orchestratorTLSName())
		b.orchestratorTLSConfigured = false
	}
	b.tlsMu.Unlock()
	return errors.Join(errs...)
}

// Exec executes the given query on the backend database.
func (b *Backend) Exec(query string, args ...interface{}) (sql.Result, error) {
	query = b.translateStatement(query)
	db, err := b.Open()
	if err != nil {
		return nil, err
	}
	return sqlutils.ExecNoPrepare(db, query, args...)
}

// QueryRowsMap runs a query without arguments, calling on_row per row.
func (b *Backend) QueryRowsMap(query string, on_row func(sqlutils.RowMap) error) error {
	query = b.translateStatement(query)
	db, err := b.Open()
	if err != nil {
		return err
	}
	return sqlutils.QueryRowsMap(db, query, on_row)
}

// Query runs a query with arguments, calling on_row per row.
func (b *Backend) Query(query string, argsArray []interface{}, on_row func(sqlutils.RowMap) error) error {
	query = b.translateStatement(query)
	db, err := b.Open()
	if err != nil {
		return err
	}
	return log.Criticale(sqlutils.QueryRowsMap(db, query, on_row, argsArray...))
}

// QueryRowsMapBuffered is QueryRowsMap reading the whole result set first.
func (b *Backend) QueryRowsMapBuffered(query string, on_row func(sqlutils.RowMap) error) error {
	query = b.translateStatement(query)
	db, err := b.Open()
	if err != nil {
		return err
	}
	return sqlutils.QueryRowsMapBuffered(db, query, on_row)
}

// QueryBuffered is Query reading the whole result set first.
func (b *Backend) QueryBuffered(query string, argsArray []interface{}, on_row func(sqlutils.RowMap) error) error {
	query = b.translateStatement(query)
	db, err := b.Open()
	if err != nil {
		return err
	}
	if argsArray == nil {
		argsArray = EmptyArgs
	}
	return log.Criticale(sqlutils.QueryRowsMapBuffered(db, query, on_row, argsArray...))
}

// ReadTimeNow reads and returns the current timestamp as string. This is an unfortunate workaround
// to support both MySQL and SQLite in all possible timezones. SQLite only speaks UTC where MySQL has
// timezone support. By reading the time as string we get the database's de-facto notion of the time,
// which we can then feed back to it.
func (b *Backend) ReadTimeNow() (timeNow string, err error) {
	err = b.Query(`select now() as time_now`, nil, func(m sqlutils.RowMap) error {
		timeNow = m.GetString("time_now")
		return nil
	})
	return timeNow, err
}

// OpenDiscovery returns a DB instance to access a topology instance with this
// tenant's credentials. It has lower read timeout than OpenTopology and is
// intended to be used with low-latency discovery queries.
func (b *Backend) OpenDiscovery(host string, port int) (*sql.DB, error) {
	return b.openTopology(host, port, b.cfg.MySQLDiscoveryReadTimeoutSeconds)
}

// OpenTopology returns a DB instance to access a topology instance with this
// tenant's credentials. The pool is owned by the Backend (not shared with other
// backends) and released by Close.
func (b *Backend) OpenTopology(host string, port int) (*sql.DB, error) {
	return b.openTopology(host, port, b.cfg.MySQLTopologyReadTimeoutSeconds)
}

func (b *Backend) openTopology(host string, port int, readTimeout int) (*sql.DB, error) {
	if b.disabled {
		return nil, ErrDefaultBackendDisabled
	}
	cfg := b.cfg
	mysql_uri := fmt.Sprintf("%s:%s@tcp(%s:%d)/?timeout=%ds&readTimeout=%ds&interpolateParams=true",
		cfg.MySQLTopologyUser,
		cfg.MySQLTopologyPassword,
		host, port,
		cfg.MySQLConnectTimeoutSeconds,
		readTimeout,
	)

	if cfg.MySQLTopologyMaxAllowedPacket >= 0 {
		mysql_uri = fmt.Sprintf("%s&maxAllowedPacket=%d", mysql_uri, cfg.MySQLTopologyMaxAllowedPacket)
	}
	if cfg.MySQLTopologyUseMutualTLS ||
		(cfg.MySQLTopologyUseMixedTLS && b.requiresTLS(host, port, mysql_uri)) {
		var err error
		if mysql_uri, err = b.setupTopologyTLS(mysql_uri); err != nil {
			return nil, err
		}
	}
	return b.topologyDB(mysql_uri, host, port)
}

// topologyDB returns the per-backend pool for the given topology DSN.
func (b *Backend) topologyDB(uri string, host string, port int) (*sql.DB, error) {
	if b.closed.Load() {
		return nil, ErrBackendClosed
	}
	b.topoMu.RLock()
	db := b.topoDBs[uri]
	b.topoMu.RUnlock()
	if db == nil {
		b.topoMu.Lock()
		if b.closed.Load() {
			b.topoMu.Unlock()
			return nil, ErrBackendClosed
		}
		if db = b.topoDBs[uri]; db == nil {
			logger := SqlUtilsLogger{tenant: b.tenantLogContext(), client_context: host + ":" + strconv.Itoa(port), backend_connection: false}
			var err error
			if db, err = sqlutils.NewDB("mysql", uri, logger); err != nil {
				b.topoMu.Unlock()
				return nil, err
			}
			if b.cfg.MySQLConnectionLifetimeSeconds > 0 {
				db.SetConnMaxLifetime(time.Duration(b.cfg.MySQLConnectionLifetimeSeconds) * time.Second)
			}
			db.SetMaxOpenConns(config.MySQLTopologyMaxPoolConnections)
			db.SetMaxIdleConns(config.MySQLTopologyMaxPoolConnections)
			b.topoDBs[uri] = db
		}
		b.topoMu.Unlock()
	}
	return db, nil
}

func (b *Backend) initOrchestratorDB(db *sql.DB) error {
	log.Debug("Initializing orchestrator")

	versionAlreadyDeployed, err := versionIsDeployed(db)
	if versionAlreadyDeployed && config.RuntimeCLIFlags.ConfiguredVersion != "" && err == nil {
		// Already deployed with this version
		return nil
	}
	if b.cfg.PanicIfDifferentDatabaseDeploy && config.RuntimeCLIFlags.ConfiguredVersion != "" && !versionAlreadyDeployed {
		return fmt.Errorf("PanicIfDifferentDatabaseDeploy is set. Configured version %s is not the version found in the database", config.RuntimeCLIFlags.ConfiguredVersion)
	}
	log.Debugf("Migrating database schema")
	if err := b.deployStatements(db, generateSQLBase); err != nil {
		return err
	}
	if err := b.deployStatements(db, generateSQLPatches); err != nil {
		return err
	}
	if err := b.registerOrchestratorDeployment(db); err != nil {
		return err
	}

	if b.cfg.IsSQLite() {
		_, _ = b.execInternal(db, `PRAGMA journal_mode = WAL`)
		_, _ = b.execInternal(db, `PRAGMA synchronous = NORMAL`)
	}
	return nil
}

// versionIsDeployed checks if given version has already been deployed
func versionIsDeployed(db *sql.DB) (result bool, err error) {
	query := `
		select
			count(*) as is_deployed
		from
			orchestrator_db_deployments
		where
			deployed_version = ?
		`
	err = db.QueryRow(query, config.RuntimeCLIFlags.ConfiguredVersion).Scan(&result)
	// err means the table 'orchestrator_db_deployments' does not even exist, in which case we proceed
	// to deploy.
	// If there's another error to this, like DB gone bad, then we're about to find out anyway.
	return result, err
}

// registerOrchestratorDeployment records the deployed version upon successful deployment
func (b *Backend) registerOrchestratorDeployment(db *sql.DB) error {
	query := `
    	replace into orchestrator_db_deployments (
				deployed_version, deployed_timestamp
			) values (
				?, NOW()
			)
				`
	if _, err := b.execInternal(db, query, config.RuntimeCLIFlags.ConfiguredVersion); err != nil {
		return fmt.Errorf("unable to write to orchestrator_db_deployments: %w", err)
	}
	log.Debugf("Migrated database schema to version [%+v]", config.RuntimeCLIFlags.ConfiguredVersion)
	return nil
}

func (b *Backend) execInternal(db *sql.DB, query string, args ...interface{}) (sql.Result, error) {
	return sqlutils.ExecNoPrepare(db, b.translateStatement(query), args...)
}

// deployStatements will issue given sql queries that are not already known to be deployed.
// This iterates both lists (to-run and already-deployed) and also verifies no contraditions.
func (b *Backend) deployStatements(db *sql.DB, queries []string) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	// Ugly workaround ahead.
	// Origin of this workaround is the existence of some "timestamp NOT NULL," column definitions,
	// where in NO_ZERO_IN_DATE,NO_ZERO_DATE sql_mode are invalid (since default is implicitly "0")
	// This means installation of orchestrator fails on such configured servers, and in particular on 5.7
	// where this setting is the dfault.
	// For purpose of backwards compatibility, what we do is force sql_mode to be more relaxed, create the schemas
	// along with the "invalid" definition, and then go ahead and fix those definitions via following ALTER statements.
	// My bad.
	originalSqlMode := ""
	if b.cfg.IsMySQL() {
		_ = tx.QueryRow(`select @@session.sql_mode`).Scan(&originalSqlMode)
		if _, err := tx.Exec(`set @@session.sql_mode=REPLACE(@@session.sql_mode, 'NO_ZERO_DATE', '')`); err != nil {
			_ = tx.Rollback()
			return err
		}
		if _, err := tx.Exec(`set @@session.sql_mode=REPLACE(@@session.sql_mode, 'NO_ZERO_IN_DATE', '')`); err != nil {
			_ = tx.Rollback()
			return err
		}
	}
	for _, query := range queries {
		query = b.translateStatement(query)
		if _, err := tx.Exec(query); err != nil {
			if strings.Contains(err.Error(), "syntax error") {
				_ = tx.Rollback()
				return fmt.Errorf("cannot initiate orchestrator: %w; query=%+v", err, query)
			}
			if !sqlutils.IsAlterTable(query) && !sqlutils.IsCreateIndex(query) && !sqlutils.IsDropIndex(query) {
				_ = tx.Rollback()
				return fmt.Errorf("cannot initiate orchestrator: %w; query=%+v", err, query)
			}
			if !strings.Contains(err.Error(), "duplicate column name") &&
				!strings.Contains(err.Error(), "Duplicate column name") &&
				!strings.Contains(err.Error(), "check that column/key exists") &&
				!strings.Contains(err.Error(), "already exists") &&
				!strings.Contains(err.Error(), "Duplicate key name") {
				_ = log.Errorf("Error initiating orchestrator: %+v; query=%+v", err, query)
			}
		}
	}
	if b.cfg.IsMySQL() {
		if _, err := tx.Exec(`set session sql_mode=?`, originalSqlMode); err != nil {
			_ = tx.Rollback()
			return err
		}
	}
	return tx.Commit()
}
