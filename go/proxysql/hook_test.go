package proxysql

import (
	"testing"

	"github.com/proxysql/orchestrator/go/config"
)

func TestHookNilClientIsNoop(t *testing.T) {
	hook := NewHook(nil, 10, 20, "offline_soft")
	err := hook.PreFailover("old-master", 3306)
	if err != nil {
		t.Errorf("expected nil error for PreFailover with nil client, got %v", err)
	}
	err = hook.PostFailover("new-master", 3306, "old-master", 3306)
	if err != nil {
		t.Errorf("expected nil error for PostFailover with nil client, got %v", err)
	}
}

func TestHookUnconfiguredHostgroupIsNoop(t *testing.T) {
	client := NewClient("127.0.0.1", 6032, "admin", "admin", false)
	hook := NewHook(client, 0, 0, "offline_soft")
	err := hook.PreFailover("old-master", 3306)
	if err != nil {
		t.Errorf("expected nil error for unconfigured hostgroups, got %v", err)
	}
}

func TestPreFailoverSQLGeneration(t *testing.T) {
	tests := []struct {
		action       string
		host         string
		port         int
		expectedSQL  string
		expectedArgs int
	}{
		{
			action:       "offline_soft",
			host:         "db1.example.com",
			port:         3306,
			expectedSQL:  "UPDATE mysql_servers SET status='OFFLINE_SOFT' WHERE hostname=? AND port=? AND hostgroup_id=?",
			expectedArgs: 3,
		},
		{
			action:       "weight_zero",
			host:         "db1.example.com",
			port:         3306,
			expectedSQL:  "UPDATE mysql_servers SET weight=0 WHERE hostname=? AND port=? AND hostgroup_id=?",
			expectedArgs: 3,
		},
		{
			action:       "none",
			host:         "db1.example.com",
			port:         3306,
			expectedSQL:  "",
			expectedArgs: 0,
		},
	}
	for _, tt := range tests {
		sql, args := buildPreFailoverSQL(tt.action, tt.host, tt.port, 10)
		if sql != tt.expectedSQL {
			t.Errorf("action=%s: expected SQL %q, got %q", tt.action, tt.expectedSQL, sql)
		}
		if len(args) != tt.expectedArgs {
			t.Errorf("action=%s: expected %d args, got %d", tt.action, tt.expectedArgs, len(args))
		}
	}
}

func TestPostFailoverSQLGeneration(t *testing.T) {
	sqls, args := buildPostFailoverSQL("new-master", 3306, "old-master", 3306, 10, 20)
	if len(sqls) < 3 {
		t.Errorf("expected at least 3 SQL statements for post-failover, got %d", len(sqls))
	}
	if len(args) != len(sqls) {
		t.Errorf("expected args slice length to match sqls length, got %d vs %d", len(args), len(sqls))
	}
}

func TestNewHookFromConfigIsPerTenant(t *testing.T) {
	cfgA := &config.Configuration{
		ProxySQLAdminAddress:      "proxysql-a.example.com",
		ProxySQLAdminPort:         6032,
		ProxySQLAdminUser:         "admin",
		ProxySQLAdminPassword:     "a",
		ProxySQLWriterHostgroup:   10,
		ProxySQLReaderHostgroup:   20,
		ProxySQLPreFailoverAction: "weight_zero",
	}
	cfgB := &config.Configuration{
		ProxySQLAdminAddress:    "proxysql-b.example.com",
		ProxySQLAdminPort:       6033,
		ProxySQLWriterHostgroup: 100,
	}
	before := GetHook()

	a := NewHookFromConfig(cfgA)
	b := NewHookFromConfig(cfgB)
	if a == b {
		t.Fatal("hooks of different tenants must be distinct")
	}
	if !a.IsConfigured() || !b.IsConfigured() {
		t.Fatalf("both hooks should be configured: a=%v b=%v", a.IsConfigured(), b.IsConfigured())
	}
	if a.WriterHostgroup() != 10 || a.ReaderHostgroup() != 20 || b.WriterHostgroup() != 100 || b.ReaderHostgroup() != 0 {
		t.Errorf("hostgroups mixed up: a=%d/%d b=%d/%d", a.WriterHostgroup(), a.ReaderHostgroup(), b.WriterHostgroup(), b.ReaderHostgroup())
	}
	if a.preFailoverAction != "weight_zero" || b.preFailoverAction != "offline_soft" {
		t.Errorf("pre-failover actions mixed up: %q %q", a.preFailoverAction, b.preFailoverAction)
	}
	if a.GetClient() == b.GetClient() || a.GetClient().address != "proxysql-a.example.com" || b.GetClient().port != 6033 {
		t.Errorf("clients must be independent: %+v %+v", a.GetClient(), b.GetClient())
	}
	if GetHook() != before {
		t.Error("NewHookFromConfig must not change the default hook")
	}
}

func TestNewHookFromConfigUnconfigured(t *testing.T) {
	for name, cfg := range map[string]*config.Configuration{
		"nil config":        nil,
		"empty config":      {},
		"no writer hg":      {ProxySQLAdminAddress: "proxysql.example.com", ProxySQLAdminPort: 6032},
		"writer hg no addr": {ProxySQLWriterHostgroup: 10},
	} {
		hook := NewHookFromConfig(cfg)
		if hook == nil || hook.IsConfigured() {
			t.Errorf("%s: expected a non-nil, unconfigured hook", name)
			continue
		}
		if err := hook.PreFailover("h", 3306); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
}

func TestNilHookIsSafe(t *testing.T) {
	var hook *Hook
	if hook.IsConfigured() || hook.WriterHostgroup() != 0 || hook.ReaderHostgroup() != 0 || hook.GetClient() != nil {
		t.Error("nil hook must be inert")
	}
}
