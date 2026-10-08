# Multi-tenant Raft chaos suite

`run-m2.sh` proves the happy path of the multi-tenant Raft (one leader, three tenants, failover, death of the
leader). This suite tries to **break** it, with real containers, under realistic faults: partitions, frozen
processes, `kill -9` in the middle of a failover, rolling restarts under traffic, real snapshot installation, a
broken tenant backend and an operator error. Every scenario prints `PASS`/`FAIL` lines with evidence and ends with
`SCENARIO-RESULT <name> PASS|FAIL`.

Nothing here edits the other scripts of this directory or the Go code; it only adds files.

| File | What it is |
|---|---|
| `lib-chaos.sh` | Helpers on top of `lib.sh`: second (Raft) network, node start, fault injection, Raft/tenant observation, traffic, evidence. |
| `chaos-0N-*.sh` | One scenario each; runnable alone (`./chaos-01-leader-partition.sh`). |
| `chaos-traffic.py` | Tenant-command load generator (host side). |
| `run-chaos.sh` | Runs the scenarios sharing one set of databases; `run-chaos.sh cleanup` removes everything. |

## Isolation

Prefix **`orcmtc`** (containers, volumes and networks `orcmtc-*`), subnets `172.31.88.0/24` (databases + HTTP) and
`172.31.89.0/24` (Raft). `lib.sh` selects with the anchored regular expression `^orcmtc-`, which cannot match
`orcmt-*` (the other suites) and vice versa; `lib-chaos.sh` refuses `ORCMT_PREFIX=orcmt`. Logs of **every** container
are saved to `$ORCMT_WORK/logs/<scenario>/` (default `/tmp/orcmtc-work`) before anything is removed. `ORCMT_KEEP=1`
leaves the containers for debugging (then `./run-chaos.sh cleanup`). Memory is bounded (`MEM_*` of `lib.sh`).

## Network model (why a second network)

Every orchestrator node has two interfaces: `orcmtc-net` (databases, HTTP clients) and `orcmtc-raftnet` (Raft bind and
`HTTPAdvertise`, i.e. the follower -> leader proxy). `tc` is not available, so faults are `docker network
disconnect/connect`, `docker pause/unpause/kill/stop/start`:

* **Raft partition** (`partition_raft`): cut from `orcmtc-raftnet` only. The node still reaches every database, which
  is the dangerous case (an isolated node that believed it was the leader would act on real masters).
* **Full isolation** (`partition_full`): cut from both networks (it sees every master dead).
* **Pause**: the process is frozen, sockets stay open, it wakes up believing nothing happened.

Every recovery hook is `/conf/hook.sh`: it records timestamp, node, **the node's Raft state at the moment the hook
runs**, tenant and phase (`pre`/`post`) in `/data/hook-<tenant>[-pre].log`. That is the evidence for "which node ran
which recovery, and was it the Leader".

## Scenarios

| # | Script | Fault | Asserts |
|---|---|---|---|
| 1 | `chaos-01-leader-partition.sh` | leader cut off (first from everything, then from Raft only while the masters of the 3 tenants crash) | the majority elects a new leader; the isolated node stops being Leader within ~1 s; **only** the new leader registers recoveries and runs hooks; the isolated one starts none; after healing it is a follower and the 3 tenants converge with exactly one recovery and one hook per tenant |
| 2 | `chaos-02-follower-partition.sh` | one follower cut off | leader unchanged; `put-instance-tag` (3 tenants), `begin-downtime`, `ack-recovery` (tenant a only) issued during the partition land on the other follower at once and on the isolated one after healing, **only in their tenant** (b's recoveries stay unacknowledged); no leakage between tenants |
| 3 | `chaos-03-leader-pause.sh` | leader frozen: 3a while two masters fail; 3b in the middle of a recovery (pre hook sleeping); 3c (`CHAOS_LONG=1`) 60 s | 3a: the woken-up node becomes Follower at once, runs no hook/recovery, nothing is duplicated; 3b: a zombie leader must not finish a recovery; 3c: it never acts as leader (and may exit, see findings) |
| 4 | `chaos-04-kill-leader-mid-failover.sh` | `kill -9` of the leader while the pre-failover hook of a recovery sleeps 20 s | measures: time until the new leader repairs the topology, hook runs (1 vs 2), `topology_recovery` rows; asserts completion, one post hook, no hook by a non-leader, rejoin and convergence |
| 5 | `chaos-05-rolling-restart.sh` | rolling restart of the 3 nodes under continuous tag traffic on the 3 tenants | every accepted command is present on all nodes; tenants identical; `untag` replication probe |
| 6 | `chaos-06-snapshot-restore.sh` | >10240 commands, forced snapshot + log compaction, 6A: a follower replaced by an empty node; 6B: only its Raft dir wiped | the leader **installs a snapshot** (v2) on the new node; per-tenant data identical and unmixed; accepted commands (snapshot + replayed tail) all present |
| 7 | `chaos-07-tenant-backend-failure.sh` | the SQLite of tenant `mariadb-b` fills its volume (6 MB tmpfs) on the leader | the leader yields in ~5 s (`yielding leadership: backend of tenant mariadb-b ...`), another node recovers **all** tenants, the broken process survives and heals; then what happened to the commands it could not apply |
| 8 | `chaos-08-tenant-mismatch.sh` | a node whose `tenants.json` lacks `mariadb-b` | `tenant unknown here` per command, the node lives and keeps applying the other tenants, nothing lands in another tenant; adding the tenant back does not recover the lost commands; wiping and re-joining does |
| 9 | `chaos-09-soak.sh` (`CHAOS_SOAK=1`) | seeded random faults (kill/pause/partition/isolation/restart + master crashes) for `CHAOS_SOAK_SECONDS` (600) | at most one Leader; no panic/fatal/`unknown tenant`; no hook by a non-leader; failovers complete; final convergence |

```sh
tests/multitenant/run-chaos.sh                 # scenarios 1-8 (~25 min)
tests/multitenant/run-chaos.sh 2 5             # selected ones
CHAOS_LONG=1 tests/multitenant/run-chaos.sh 3  # + the 60 s freeze
CHAOS_SOAK=1 CHAOS_SOAK_SECONDS=600 CHAOS_SEED=42 tests/multitenant/run-chaos.sh 9
tests/multitenant/run-chaos.sh cleanup
```

Knobs: `T_*` and `RECOVERY_BLOCK_SECONDS` of `lib.sh`, `S4_BLOCK_SECONDS` (60), `S4_WAIT` (240), `S6_OPS` (11500),
`CHAOS_PRE_SLEEP_A`, `CHAOS_TMPFS_B`, `CHAOS_DROP_TENANT_N3`, `CHAOS_SEED`, `ORCMT_REBUILD=1` (rebuild the binary after
changing Go code).

Snapshot threshold: `store.go` fixes `SnapshotThreshold=1`, `SnapshotInterval=30m` and the hashicorp default
`TrailingLogs=10240`; nothing is configurable, so scenario 6 publishes >10240 real commands and checks that the
leader's `raft_log` first index moved past 1 before it wipes a node.

## Findings

See the report of the run (also summarised in the commit messages). Those that come from the binary and not from the
scripts are listed here so they are not lost; none was fixed (out of scope). Items marked *pre-existing* exist
identically in the legacy single-tenant code.

1. **A recovery in flight is not aborted when the leader loses the term** (scenario 3b, *pre-existing design*).
   `executeCheckAndRecoverFunction` checks `t.isLeader()` once (`go/logic/topology_recovery.go:1911`); pre hooks,
   promotion, KV writes and post hooks then run on whatever state the node has. Observed: a leader frozen for 24 s
   in its pre-failover hook woke up as Follower, promoted the replica and ran the post hook (`state=Follower`); every
   Raft write it tried failed with `not leader`, so the recovery row was never resolved on any node.
2. **An orphan recovery is not recognised as crashed** (scenario 4, *pre-existing*). Replicated rows are stamped with the
   applying node's hostname/token (`topology_recovery_dao.go:180`), so `AcknowledgeCrashedRecoveries`
   (`topology_recovery_dao.go:471`, joins with `node_health`) never matches: after `kill -9` of the leader the new
   leader waits the whole `RecoveryPeriodBlockSeconds` before repeating the recovery (pre hook ran 2 times, post hook once).
3. **`Untag`/`UntagAll` are not replicated** (scenario 5b, *pre-existing*): `go/http/api.go` calls `tenant.Store.Untag`
   directly; the `delete-instance-tag` op the applier understands (`command_applier.go:150`) is never published.
4. **Commands a node fails to apply are never retried** (scenario 7): with its tenant backend full the node answers an
   error to the FSM and moves on; Raft snapshots are published after every recovery (`topology_recovery.go:986`), so the
   node later restores a snapshot built from the backend that had missed them. Only wiping the node (Raft dir and tenant
   backends) rebuilds it.
5. `UnknownTenantCommands()` (`go/raft/tenant.go:78`) is counted but exported nowhere: the only trace of a tenants-file
   mismatch is the log line.
6. The `lost-in-recovery` downtime is written by the recovering leader locally, not through Raft (*pre-existing*), so it
   is excluded from the convergence fingerprint.
7. A process frozen for longer than `fatalAfterUnhealthyDuration` (30 s) may `log.Fatal` on wake-up (`node is unable to
   register health`); whether it does is a race between the health tick and the heartbeat goroutine (*pre-existing*).
