package config

import (
	"reflect"
	"sort"
	"strings"
)

// Scope classifies a Configuration field by who owns its value in a
// multi-tenant deployment.
type Scope int

const (
	// ScopeGlobal fields must be identical on every Raft node and are shared by
	// all tenants (single HTTP server, authentication, Raft membership, agents...).
	// They are never overridable per tenant.
	ScopeGlobal Scope = iota + 1
	// ScopeNodeLocal fields legitimately differ per node (binds, TLS files,
	// logging, process metrics...) and are shared by all tenants of the process.
	ScopeNodeLocal
	// ScopeTenant fields can be overridden per tenant (credentials, queries,
	// filters, hooks, ProxySQL, KV...). They go in the "Config" section of a tenant.
	ScopeTenant
	// ScopeBackendNodeLocal fields describe the orchestrator backend database of
	// the node. In multi-tenant each tenant has its own backend, so they can be
	// overridden per tenant in the "Backend" section of a tenant.
	ScopeBackendNodeLocal
)

func (s Scope) String() string {
	switch s {
	case ScopeGlobal:
		return "global"
	case ScopeNodeLocal:
		return "node-local"
	case ScopeTenant:
		return "tenant-scoped"
	case ScopeBackendNodeLocal:
		return "backend node-local"
	}
	return "unclassified"
}

// fieldScope classifies EVERY field of Configuration. TestFieldScopeCoversAllFields
// fails when a field is added to Configuration without an entry here, so that
// the decision "can a tenant override this?" is always taken explicitly.
var fieldScope = map[string]Scope{
	"Debug":                                      ScopeNodeLocal,
	"EnableSyslog":                               ScopeNodeLocal,
	"ListenAddress":                              ScopeNodeLocal,
	"ListenSocket":                               ScopeNodeLocal,
	"HTTPAdvertise":                              ScopeNodeLocal,
	"AgentsServerPort":                           ScopeNodeLocal,
	"MySQLTopologyUser":                          ScopeTenant,
	"MySQLTopologyPassword":                      ScopeTenant,
	"PostgreSQLTopologyUser":                     ScopeTenant,
	"PostgreSQLTopologyPassword":                 ScopeTenant,
	"PostgreSQLSSLMode":                          ScopeTenant,
	"ProviderType":                               ScopeTenant,
	"MySQLTopologyCredentialsConfigFile":         ScopeTenant,
	"MySQLTopologySSLPrivateKeyFile":             ScopeTenant,
	"MySQLTopologySSLCertFile":                   ScopeTenant,
	"MySQLTopologySSLCAFile":                     ScopeTenant,
	"MySQLTopologySSLSkipVerify":                 ScopeTenant,
	"MySQLTopologyUseMutualTLS":                  ScopeTenant,
	"MySQLTopologyUseMixedTLS":                   ScopeTenant,
	"MySQLTopologyMaxAllowedPacket":              ScopeTenant,
	"TLSCacheTTLFactor":                          ScopeTenant,
	"BackendDB":                                  ScopeBackendNodeLocal,
	"SQLite3DataFile":                            ScopeBackendNodeLocal,
	"SkipOrchestratorDatabaseUpdate":             ScopeNodeLocal,
	"PanicIfDifferentDatabaseDeploy":             ScopeNodeLocal,
	"RaftEnabled":                                ScopeGlobal,
	"RaftBind":                                   ScopeNodeLocal,
	"RaftAdvertise":                              ScopeNodeLocal,
	"RaftDataDir":                                ScopeNodeLocal,
	"DefaultRaftPort":                            ScopeGlobal,
	"RaftNodes":                                  ScopeGlobal,
	"ExpectFailureAnalysisConcensus":             ScopeGlobal,
	"MySQLOrchestratorHost":                      ScopeBackendNodeLocal,
	"MySQLOrchestratorMaxPoolConnections":        ScopeBackendNodeLocal,
	"MySQLOrchestratorPort":                      ScopeBackendNodeLocal,
	"MySQLOrchestratorDatabase":                  ScopeBackendNodeLocal,
	"MySQLOrchestratorUser":                      ScopeBackendNodeLocal,
	"MySQLOrchestratorPassword":                  ScopeBackendNodeLocal,
	"MySQLOrchestratorCredentialsConfigFile":     ScopeBackendNodeLocal,
	"MySQLOrchestratorSSLPrivateKeyFile":         ScopeBackendNodeLocal,
	"MySQLOrchestratorSSLCertFile":               ScopeBackendNodeLocal,
	"MySQLOrchestratorSSLCAFile":                 ScopeBackendNodeLocal,
	"MySQLOrchestratorSSLSkipVerify":             ScopeBackendNodeLocal,
	"MySQLOrchestratorUseMutualTLS":              ScopeBackendNodeLocal,
	"MySQLOrchestratorReadTimeoutSeconds":        ScopeBackendNodeLocal,
	"MySQLOrchestratorRejectReadOnly":            ScopeBackendNodeLocal,
	"MySQLOrchestratorMaxAllowedPacket":          ScopeBackendNodeLocal,
	"MySQLConnectTimeoutSeconds":                 ScopeTenant,
	"MySQLDiscoveryReadTimeoutSeconds":           ScopeTenant,
	"MySQLTopologyReadTimeoutSeconds":            ScopeTenant,
	"MySQLConnectionLifetimeSeconds":             ScopeTenant,
	"DefaultInstancePort":                        ScopeTenant,
	"SlaveLagQuery":                              ScopeTenant,
	"ReplicationLagQuery":                        ScopeTenant,
	"ReplicationCredentialsQuery":                ScopeTenant,
	"DiscoverByShowSlaveHosts":                   ScopeTenant,
	"UseSuperReadOnly":                           ScopeTenant,
	"InstancePollSeconds":                        ScopeTenant,
	"DeadInstancePollSecondsMultiplyFactor":      ScopeTenant,
	"DeadInstancePollSecondsMax":                 ScopeTenant,
	"DeadInstanceDiscoveryMaxConcurrency":        ScopeTenant,
	"DeadInstanceDiscoveryLogsEnabled":           ScopeNodeLocal,
	"ReasonableInstanceCheckSeconds":             ScopeTenant,
	"InstanceWriteBufferSize":                    ScopeNodeLocal,
	"BufferInstanceWrites":                       ScopeNodeLocal,
	"InstanceFlushIntervalMilliseconds":          ScopeNodeLocal,
	"SkipMaxScaleCheck":                          ScopeTenant,
	"LowerReplicaVersionAllowed":                 ScopeTenant,
	"UnseenInstanceForgetHours":                  ScopeTenant,
	"SnapshotTopologiesIntervalHours":            ScopeGlobal,
	"DiscoveryMaxConcurrency":                    ScopeTenant,
	"DiscoveryQueueCapacity":                     ScopeTenant,
	"DiscoveryQueueMaxStatisticsSize":            ScopeNodeLocal,
	"DiscoveryCollectionRetentionSeconds":        ScopeNodeLocal,
	"DiscoverySeeds":                             ScopeTenant,
	"InstanceBulkOperationsWaitTimeoutSeconds":   ScopeTenant,
	"HostnameResolveMethod":                      ScopeTenant,
	"MySQLHostnameResolveMethod":                 ScopeTenant,
	"SkipBinlogServerUnresolveCheck":             ScopeTenant,
	"ExpiryHostnameResolvesMinutes":              ScopeTenant,
	"RejectHostnameResolvePattern":               ScopeTenant,
	"ReasonableReplicationLagSeconds":            ScopeTenant,
	"ProblemIgnoreHostnameFilters":               ScopeTenant,
	"VerifyReplicationFilters":                   ScopeTenant,
	"ReasonableMaintenanceReplicationLagSeconds": ScopeTenant,
	"CandidateInstanceExpireMinutes":             ScopeTenant,
	"AuditLogFile":                               ScopeNodeLocal,
	"AuditToSyslog":                              ScopeNodeLocal,
	"AuditToBackendDB":                           ScopeGlobal,
	"AuditPurgeDays":                             ScopeTenant,
	"RemoveTextFromHostnameDisplay":              ScopeTenant,
	"ReadOnly":                                   ScopeGlobal,
	"AuthenticationMethod":                       ScopeGlobal,
	"OAuthClientId":                              ScopeGlobal,
	"OAuthClientSecret":                          ScopeGlobal,
	"OAuthScopes":                                ScopeGlobal,
	"HTTPAuthUser":                               ScopeGlobal,
	"HTTPAuthPassword":                           ScopeGlobal,
	"AuthUserHeader":                             ScopeGlobal,
	"PowerAuthUsers":                             ScopeGlobal,
	"PowerAuthGroups":                            ScopeGlobal,
	"AccessTokenUseExpirySeconds":                ScopeGlobal,
	"AccessTokenExpiryMinutes":                   ScopeGlobal,
	"ClusterNameToAlias":                         ScopeTenant,
	"DetectClusterAliasQuery":                    ScopeTenant,
	"DetectClusterDomainQuery":                   ScopeTenant,
	"DetectInstanceAliasQuery":                   ScopeTenant,
	"DetectPromotionRuleQuery":                   ScopeTenant,
	"DataCenterPattern":                          ScopeTenant,
	"RegionPattern":                              ScopeTenant,
	"PhysicalEnvironmentPattern":                 ScopeTenant,
	"DetectDataCenterQuery":                      ScopeTenant,
	"DetectRegionQuery":                          ScopeTenant,
	"DetectPhysicalEnvironmentQuery":             ScopeTenant,
	"DetectSemiSyncEnforcedQuery":                ScopeTenant,
	"SupportFuzzyPoolHostnames":                  ScopeTenant,
	"InstancePoolExpiryMinutes":                  ScopeTenant,
	"PromotionIgnoreHostnameFilters":             ScopeTenant,
	"ServeAgentsHttp":                            ScopeNodeLocal,
	"AgentsUseSSL":                               ScopeNodeLocal,
	"AgentsUseMutualTLS":                         ScopeNodeLocal,
	"AgentSSLSkipVerify":                         ScopeNodeLocal,
	"AgentSSLPrivateKeyFile":                     ScopeNodeLocal,
	"AgentSSLCertFile":                           ScopeNodeLocal,
	"AgentSSLCAFile":                             ScopeNodeLocal,
	"AgentSSLValidOUs":                           ScopeNodeLocal,
	"UseSSL":                                     ScopeNodeLocal,
	"UseMutualTLS":                               ScopeNodeLocal,
	"SSLSkipVerify":                              ScopeNodeLocal,
	"SSLPrivateKeyFile":                          ScopeNodeLocal,
	"SSLCertFile":                                ScopeNodeLocal,
	"SSLCAFile":                                  ScopeNodeLocal,
	"SSLValidOUs":                                ScopeNodeLocal,
	"StatusEndpoint":                             ScopeGlobal,
	"StatusOUVerify":                             ScopeNodeLocal,
	"AgentPollMinutes":                           ScopeGlobal,
	"UnseenAgentForgetHours":                     ScopeGlobal,
	"StaleSeedFailMinutes":                       ScopeGlobal,
	"SeedAcceptableBytesDiff":                    ScopeGlobal,
	"SeedWaitSecondsBeforeSend":                  ScopeGlobal,
	"AutoPseudoGTID":                             ScopeTenant,
	"PseudoGTIDPattern":                          ScopeTenant,
	"PseudoGTIDPatternIsFixedSubstring":          ScopeTenant,
	"PseudoGTIDMonotonicHint":                    ScopeTenant,
	"DetectPseudoGTIDQuery":                      ScopeTenant,
	"BinlogEventsChunkSize":                      ScopeTenant,
	"SkipBinlogEventsContaining":                 ScopeTenant,
	"ReduceReplicationAnalysisCount":             ScopeTenant,
	"FailureDetectionPeriodBlockMinutes":         ScopeTenant,
	"RecoveryPeriodBlockMinutes":                 ScopeTenant,
	"RecoveryPeriodBlockSeconds":                 ScopeTenant,
	"RecoveryIgnoreHostnameFilters":              ScopeTenant,
	"RecoverMasterClusterFilters":                ScopeTenant,
	"RecoverIntermediateMasterClusterFilters":    ScopeTenant,
	"ProcessesShellCommand":                      ScopeNodeLocal,
	"OnFailureDetectionProcesses":                ScopeTenant,
	"PreGracefulTakeoverProcesses":               ScopeTenant,
	"PreFailoverProcesses":                       ScopeTenant,
	"PostFailoverProcesses":                      ScopeTenant,
	"PostUnsuccessfulFailoverProcesses":          ScopeTenant,
	"PostMasterFailoverProcesses":                ScopeTenant,
	"PostIntermediateMasterFailoverProcesses":    ScopeTenant,
	"PostGracefulTakeoverProcesses":              ScopeTenant,
	"PostTakeMasterProcesses":                    ScopeTenant,
	"FailRecoveryOnFailedPostFailoverProcesses":  ScopeTenant,
	"RecoverNonWriteableMaster":                  ScopeTenant,
	"CoMasterRecoveryMustPromoteOtherCoMaster":   ScopeTenant,
	"DetachLostSlavesAfterMasterFailover":        ScopeTenant,
	"DetachLostReplicasAfterMasterFailover":      ScopeTenant,
	"ApplyMySQLPromotionAfterMasterFailover":     ScopeTenant,
	"PreventCrossDataCenterMasterFailover":       ScopeTenant,
	"PreventCrossRegionMasterFailover":           ScopeTenant,
	"MasterFailoverLostInstancesDowntimeMinutes": ScopeTenant,
	"MasterFailoverDetachSlaveMasterHost":        ScopeTenant,
	"MasterFailoverDetachReplicaMasterHost":      ScopeTenant,
	"FailMasterPromotionOnLagMinutes":            ScopeTenant,
	"FailMasterPromotionIfSQLThreadNotUpToDate":  ScopeTenant,
	"DelayMasterPromotionIfSQLThreadNotUpToDate": ScopeTenant,
	"PostponeSlaveRecoveryOnLagMinutes":          ScopeTenant,
	"PostponeReplicaRecoveryOnLagMinutes":        ScopeTenant,
	"OSCIgnoreHostnameFilters":                   ScopeTenant,
	"GraphiteAddr":                               ScopeNodeLocal,
	"GraphitePath":                               ScopeNodeLocal,
	"GraphiteConvertHostnameDotsToUnderscores":   ScopeNodeLocal,
	"GraphitePollSeconds":                        ScopeNodeLocal,
	"URLPrefix":                                  ScopeGlobal,
	"DiscoveryIgnoreReplicaHostnameFilters":      ScopeTenant,
	"DiscoveryIgnoreMasterHostnameFilters":       ScopeTenant,
	"DiscoveryIgnoreHostnameFilters":             ScopeTenant,
	"DiscoveryIgnoreReplicationUsernameFilters":  ScopeTenant,
	"EnableDiscoveryFiltersLogs":                 ScopeNodeLocal,
	"ConsulAddress":                              ScopeTenant,
	"ConsulScheme":                               ScopeTenant,
	"ConsulAclToken":                             ScopeTenant,
	"ConsulCrossDataCenterDistribution":          ScopeTenant,
	"ConsulKVStoreProvider":                      ScopeTenant,
	"ConsulMaxKVsPerTransaction":                 ScopeTenant,
	"ZkAddress":                                  ScopeTenant,
	"KVClusterMasterPrefix":                      ScopeTenant,
	"WebMessage":                                 ScopeGlobal,
	"MaxConcurrentReplicaOperations":             ScopeTenant,
	"EnforceExactSemiSyncReplicas":               ScopeTenant,
	"RecoverLockedSemiSyncMaster":                ScopeTenant,
	"ReasonableLockedSemiSyncMasterSeconds":      ScopeTenant,
	"PrependMessagesWithOrcIdentity":             ScopeNodeLocal,
	"CustomOrcIdentity":                          ScopeNodeLocal,
	"ProxySQLAdminAddress":                       ScopeTenant,
	"ProxySQLAdminPort":                          ScopeTenant,
	"ProxySQLAdminUser":                          ScopeTenant,
	"ProxySQLAdminPassword":                      ScopeTenant,
	"ProxySQLAdminUseTLS":                        ScopeTenant,
	"ProxySQLWriterHostgroup":                    ScopeTenant,
	"ProxySQLReaderHostgroup":                    ScopeTenant,
	"ProxySQLPreFailoverAction":                  ScopeTenant,
	"PrometheusEnabled":                          ScopeNodeLocal,
	"TenantsConfigFile":                          ScopeNodeLocal,
}

// FieldScope returns the scope of a Configuration field (by exact Go field name).
func FieldScope(field string) (Scope, bool) {
	s, ok := fieldScope[field]
	return s, ok
}

// IsTenantScoped reports whether the field can be overridden per tenant ("Config" section).
func IsTenantScoped(field string) bool { return fieldScope[field] == ScopeTenant }

// IsGlobal reports whether the field is cluster-global (identical on all nodes, shared by all tenants).
func IsGlobal(field string) bool { return fieldScope[field] == ScopeGlobal }

// IsNodeLocal reports whether the field is node-local and not related to the backend database.
func IsNodeLocal(field string) bool { return fieldScope[field] == ScopeNodeLocal }

// IsBackendNodeLocal reports whether the field describes the backend database ("Backend" section of a tenant).
func IsBackendNodeLocal(field string) bool { return fieldScope[field] == ScopeBackendNodeLocal }

// FieldsWithScope returns the sorted names of all Configuration fields with the given scope.
func FieldsWithScope(s Scope) []string {
	var fields []string
	for name, fs := range fieldScope {
		if fs == s {
			fields = append(fields, name)
		}
	}
	sort.Strings(fields)
	return fields
}

// canonicalFieldNames maps the lower-cased field name to its exact name, because
// encoding/json matches keys case-insensitively and tenant manifests follow the same rule.
var canonicalFieldNames = func() map[string]string {
	m := make(map[string]string, len(fieldScope))
	t := reflect.TypeOf(Configuration{})
	for i := 0; i < t.NumField(); i++ {
		m[strings.ToLower(t.Field(i).Name)] = t.Field(i).Name
	}
	return m
}()
