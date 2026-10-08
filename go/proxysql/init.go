package proxysql

import (
	"sync"
	"sync/atomic"

	"github.com/proxysql/golib/log"
	"github.com/proxysql/orchestrator/go/config"
)

var (
	hookOnce    sync.Once
	defaultHook atomic.Pointer[Hook]
)

func init() {
	defaultHook.Store(NewHook(nil, 0, 0, ""))
}

// NewHookFromConfig builds the ProxySQL hook of one tenant from its configuration.
// It never returns nil: an unconfigured hook (no admin address or no writer hostgroup)
// is inactive and all its operations are no-ops. Nothing is stored at package level.
func NewHookFromConfig(cfg *config.Configuration) *Hook {
	if cfg == nil {
		return NewHook(nil, 0, 0, "")
	}
	client := NewClient(
		cfg.ProxySQLAdminAddress,
		cfg.ProxySQLAdminPort,
		cfg.ProxySQLAdminUser,
		cfg.ProxySQLAdminPassword,
		cfg.ProxySQLAdminUseTLS,
	)
	hook := NewHook(
		client,
		cfg.ProxySQLWriterHostgroup,
		cfg.ProxySQLReaderHostgroup,
		cfg.ProxySQLPreFailoverAction,
	)
	if hook.IsConfigured() {
		log.Infof("ProxySQL hooks enabled: admin=%s:%d writer_hg=%d reader_hg=%d",
			cfg.ProxySQLAdminAddress,
			cfg.ProxySQLAdminPort,
			cfg.ProxySQLWriterHostgroup,
			cfg.ProxySQLReaderHostgroup,
		)
	} else if cfg.ProxySQLAdminAddress != "" && cfg.ProxySQLWriterHostgroup == 0 {
		_ = log.Warningf("ProxySQL: ProxySQLAdminAddress is set but ProxySQLWriterHostgroup is 0 (unconfigured). ProxySQL hooks will be inactive.")
	}
	return hook
}

// InitHook builds the default hook from config.Config, once in the lifetime of the app
// (legacy, single tenant shim over NewHookFromConfig).
func InitHook() {
	hookOnce.Do(func() {
		defaultHook.Store(NewHookFromConfig(config.Config))
	})
}

// GetHook returns the default hook (legacy, single tenant); unconfigured until InitHook has run.
func GetHook() *Hook {
	return defaultHook.Load()
}
