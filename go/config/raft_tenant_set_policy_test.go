package config

import "testing"

func TestRaftTenantSetMismatchPolicyValidation(t *testing.T) {
	if got := newConfiguration().RaftTenantSetMismatchPolicy; got != TenantSetMismatchWarn {
		t.Errorf("default policy = %q, want %q", got, TenantSetMismatchWarn)
	}
	for _, tc := range []struct {
		in, want string
		wantErr  bool
	}{{"", "warn", false}, {"WARN", "warn", false}, {" yield ", "yield", false}, {"abort", "", true}} {
		cfg := newConfiguration()
		cfg.RaftTenantSetMismatchPolicy = tc.in
		err := cfg.adjustNodeLevel()
		if (err != nil) != tc.wantErr || (err == nil && cfg.RaftTenantSetMismatchPolicy != tc.want) {
			t.Errorf("policy %q: got %q, err %v", tc.in, cfg.RaftTenantSetMismatchPolicy, err)
		}
	}
}
