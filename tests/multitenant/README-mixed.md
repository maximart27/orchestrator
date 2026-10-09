# Mixed-version (rolling upgrade) tests

`run-mixed.sh` puts one **OLD** orchestrator (upstream `master`, v4.32.0 base, no tenants) and the **NEW** one (this tree)
in the same Raft group of three nodes (`orcmtv-n1..n3`) over one MariaDB topology (`orcmtv-a1 -> a2`), on the same
data volumes, and checks that they interoperate. Everything is named `orcmtv-*` (own network `172.31.99.0/24`, own work dir
`/tmp/orcmtv-work`); nothing else is touched. It is sized for a small PC: 256 MB per container, MariaDB buffer pool 32 MB, and
it waits (up to 10 min) until 1500 MB are available before starting each scenario.

| # | Scenario | What it proves |
|---|---|---|
| 1 | **Rolling upgrade, legacy config** (no tenants file): OLD x3 -> follower NEW -> 2nd follower NEW -> leader NEW | one leader and one converged state in every phase (`database_instance_tags`, downtime, candidate, alias override, unresolve, recoveries), the failover hook runs exactly once, on the leader, in the all-OLD, OLD-leader/NEW-followers and all-NEW phases; no `Unknown command op`; no Raft entry carries a `tenant` field |
| 2 | **Legacy commands across versions** in both directions (NEW leader + OLD follower, OLD leader + NEW followers) | begin/end-downtime, put-instance-tag, register-candidate, set-cluster-alias, register-hostname-unresolve, enable/disable-global-recoveries, ack-recovery (by instance / all), forget: same effect on OLD and NEW; the log envelope of the legacy tenant is byte for byte the old one |
| 3 | **Snapshots across versions**: a follower of the other version starts empty and the leader can only send an `InstallSnapshot` (the leader's log is truncated before the snapshot, as raft does after `TrailingLogs` entries) | NEW -> OLD and OLD -> NEW restore the full state, including the disabled-recoveries flag |
| 4 | **Hazard**: tenants file with a 2nd tenant on n2/n3 (NEW) while n1 is OLD | reproduces that n1 applies the commands of `tenant-b` to its single backend: **upgrade every node before activating a second tenant** |
| 5 | **Downgrade** of one node NEW -> OLD in legacy mode | the OLD binary starts on the Raft dir (log + snapshot) written by NEW and converges |

```sh
tests/multitenant/run-mixed.sh            # everything (~20 min)
tests/multitenant/run-mixed.sh 1 3        # some scenarios
ORCMT_KEEP=1 tests/multitenant/run-mixed.sh 2   # leave the containers for debugging (then ./cleanup.sh with ORCMT_PREFIX=orcmtv)
ORCMT_REBUILD=1 ...                       # rebuild the OLD binary (from `git archive master`; vendor/ is taken from this tree, same go.mod)
```

Logs of every node incarnation (a node swapped to another binary keeps the log of the previous one as `<node>.old.log` /
`<node>.new.log`) are kept in `/tmp/orcmtv-work/logs/s<N>/`.

## Findings that shaped the tests

* **Snapshot compatibility (fixed).** A version 2 snapshot (`Tenants` map) read by an OLD node is an empty version 1
  snapshot: nothing of the replicated tables is restored and the *disabled recoveries* flag is reset. A legacy
  single-tenant deployment now keeps writing version 1 (see `go/logic/snapshot_data.go`, `GetData`); version 2 only appears with
  a second tenant. Scenarios 3 and 5 fail on a tree without that fix.
* `/api/untag` is applied on the node that receives the call and is never published to Raft, and `submit-pool-instances`
  leaves nothing readable in the SQLite backend: both behave like that in OLD and NEW, so they are not part of the battery.
* `ack-recovery` **by cluster name** also matches on the cluster alias each node derives from its own discovery state, so the
  nodes (of any version) can disagree; the tests acknowledge by instance / all.
* A downtimed master or replica is not recovered: the phases end their downtimes before the failover.
