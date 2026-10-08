#!/usr/bin/env bash
# Scenario 6 -- REAL SNAPSHOT / RESTORE (format v2, one section per tenant).
# The snapshot threshold is NOT configurable (go/raft/store.go: SnapshotThreshold=1, SnapshotInterval=30m, and the
# hashicorp default TrailingLogs=10240 decides when the log is truncated), so the log is made long enough for real:
# more than 10240 tenant commands are published (tags on the 3 tenants), a snapshot is forced through
# /api/raft-snapshot, and the leader's log is checked to be compacted (first index > 1).
#   6A  a follower is replaced by a brand new node (EMPTY /data: new Raft state AND empty tenant backends, same bind):
#       the leader has no log to replay, so it must install the snapshot; every tenant must converge with its own data
#       and nothing from another tenant.
#   6B  another follower loses ONLY its Raft directory but keeps its tenant SQLite files (a stale but populated backend),
#       while more commands are published and a new snapshot is taken: the snapshot restore into the populated backend must
#       converge with the cluster and mix nothing. (Rows deleted meanwhile cannot be tested: Untag is not replicated.)
source "$(dirname "$0")/lib-chaos.sh"
scenario_begin "06-snapshot-restore"
S6_OPS="${S6_OPS:-11500}"
chaos_setup a b c
nodes_fresh
L="$LEADER"; V1="${FOLLOWERS[0]}"; V2="${FOLLOWERS[1]}"
log "leader=$L victims: $V1 (6A, wiped completely) and $V2 (6B, only /data/raft wiped)"
raft_db() { sql "$1" /data/raft/raft_store.db "$2" 2>/dev/null; }
snap_json() { docker exec "$(cname $L)" sh -c 'f=$(ls -t /data/raft/snapshots/*/state.bin 2>/dev/null | head -1); [ -n "$f" ] && gzip -dc "$f"'; }

# ---- 1. enough log for a real compaction
T1=$SECONDS
traffic_start s6 "$ORCMT_WORK/traffic-6a.jsonl" 8 0 "$S6_OPS"
GEN1="$CHAOS_GEN_PID"; while kill -0 "$GEN1" 2>/dev/null; do sleep 2; [ $((SECONDS-T1)) -gt 600 ] && break; done
log "published $(grep -c '"kind": "ok"' "$ORCMT_WORK/traffic-6a.jsonl") commands in $((SECONDS-T1))s: $(traffic_stats)"
for t in "${TENANTS[@]}"; do wait_until 120 "$t converged" converged "$t" "${NODES[@]}" || fail "$t: nodes do not converge after the bulk load"; done
last="$(raft_db "$L" 'select max(log_index) from raft_log')"
[ "${last:-0}" -gt 10300 ] && pass "leader log long enough to be compacted (last index $last > TrailingLogs 10240)" || fail "leader log too short for compaction (last index ${last:-?})"
for n in "${NODES[@]}"; do api "$n" /api/raft-snapshot >/dev/null; done
first_ok() { [ "$(raft_db "$L" 'select min(log_index) from raft_log')" -gt 1 ] 2>/dev/null; }
wait_until 60 "leader log compacted" first_ok && pass "snapshot taken and log compacted: leader first index = $(raft_db "$L" 'select min(log_index) from raft_log')" || fail "leader log not compacted after the snapshot"
sj="$(snap_json)"
echo "$sj" | grep -q '"FormatVersion":2' && pass "snapshot is format v2" || fail "snapshot not v2"
printf '%s' "$sj" > "$ORCMT_WORK/snapshot-6.json"
python3 - "$ORCMT_WORK/snapshot-6.json" <<'PY' && pass "snapshot sections are tenant-pure (every tag of a tenant's section carries its own tenant suffix)" || fail "snapshot sections mix tenants"
import json, sys
d = json.load(open(sys.argv[1])); ok = True
for tid, ts in d["Tenants"].items():
    rows = ts["InstanceTags"]
    cols = rows["Columns"]; i = cols.index("tag_name")
    names = [r[i] for r in rows["Data"]] if rows.get("Data") else []
    bad = [n for n in names if n.startswith("s6-") and not n.endswith("_" + tid)]
    print("snapshot tenant %s: %d tags, %d foreign" % (tid, len(names), len(bad)), file=sys.stderr)
    ok = ok and not bad
sys.exit(0 if ok else 1)
PY

# ---- 6A
echo "== 6A: follower $V1 replaced by an empty node ==" >&2
node_stop "$V1" 10
traffic_start w6 "$ORCMT_WORK/traffic-6w.jsonl" 3 0.05 150; GENW="$CHAOS_GEN_PID"; while kill -0 "$GENW" 2>/dev/null; do sleep 1; done
docker rm -f -v "$(cname $V1)" >/dev/null 2>&1; node_remove_volume "$V1"
cnode_start "$V1"
wait_until "$T_NODE_READY" "$V1 HTTP" node_http_ready "$V1" || { node_logs "$V1" | tail -20 >&2; fail "$V1 did not start"; }
cluster_stable "${NODES[@]}" && pass "$V1 (fresh data dir, same bind) joined: $(states_line "${NODES[@]}")" || fail "cluster not stable after replacing $V1"
for t in "${TENANTS[@]}"; do
  wait_until 120 "$t: $V1 converged with the cluster" converged "$t" "${NODES[@]}" && pass "$t: $V1 identical to the other nodes (fingerprint)" \
    || fail "$t: $V1 diverges" "$(for n in "${NODES[@]}"; do echo "$n=$(tenant_fp $n $t) tags=$(tag_count $n $t)"; done | tr '\n' ' ')"
done
log "evidence: leader '$(node_logs "$LEADER" | grep -c 'installing snapshot on')' x 'installing snapshot on', $V1: $(node_logs "$V1" | grep -c -i 'snapshot') snapshot lines"
node_logs "$LEADER" | grep 'installing snapshot on' | tail -2 | cut -c1-200 >&2
node_logs "$LEADER" | grep -q "installing snapshot on.*$(raft_ip $V1)" && pass "the leader installed a snapshot on $V1 (not just a log replay)" || fail "no snapshot installation on $V1 in the leader log (log replay instead?)"
CHAOS_GEN_LOG="$ORCMT_WORK/traffic-6a.jsonl"; traffic_verify s6 "$V1"; m1=$TRAFFIC_MISSING
CHAOS_GEN_LOG="$ORCMT_WORK/traffic-6w.jsonl"; traffic_verify w6 "$V1"; m2=$TRAFFIC_MISSING
assert_eq "$V1: no accepted tag missing (snapshot part + replayed tail)" 0 "$((m1+m2))"
for t in "${TENANTS[@]}"; do
  foreign="$(sql "$V1" "$(tdb "$t")" "select count(*) from database_instance_tags where (tag_name like 's6-%' or tag_name like 'w6-%') and tag_name not like '%\\_$t' escape '\\'")"
  assert_eq "$t on $V1: no tag of another tenant (no mixing)" 0 "$foreign"
  assert_eq "$t: tag count equals the leader's" "$(tag_count "$L" "$t")" "$(tag_count "$V1" "$t")"
done

# ---- 6B
echo "== 6B: $V2 loses only its Raft directory (tenant backends kept) while more commands are published ==" >&2
node_stop "$V2" 10
traffic_start k6 "$ORCMT_WORK/traffic-6k.jsonl" 3 0.05 200; GENK="$CHAOS_GEN_PID"; while kill -0 "$GENK" 2>/dev/null; do sleep 1; done
api "$L" /api/raft-snapshot >/dev/null
docker run --rm -v "$(cname $V2)-data:/data" "$IMG_GO" rm -rf /data/raft
node_up "$V2"
wait_until "$T_NODE_READY" "$V2 HTTP" node_http_ready "$V2" || fail "$V2 did not start"
cluster_stable "${NODES[@]}" && pass "$V2 rejoined with an empty Raft dir: $(states_line "${NODES[@]}")" || fail "cluster not stable after wiping $V2's raft dir"
for t in "${TENANTS[@]}"; do
  wait_until 90 "$t converged (6B)" converged "$t" "${NODES[@]}" && pass "$t: $V2 identical to the cluster after restoring into its populated backend" \
    || fail "$t: $V2 diverges after restoring only its raft dir" "$(for n in "${NODES[@]}"; do echo "$n tags=$(tag_count $n $t)"; done | tr '\n' ' ')"
done
CHAOS_GEN_LOG="$ORCMT_WORK/traffic-6k.jsonl"; traffic_verify k6 "$V2"
assert_eq "$V2: no accepted command of the 200 published while it was down is missing" 0 "$TRAFFIC_MISSING"
assert_clean_logs "end" "${NODES[@]}"
scenario_verdict; summary
