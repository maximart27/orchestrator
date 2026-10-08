#!/bin/bash
#
# Tests the tenant support of orchestrator-client against a fake HTTP server.
# The fake server records every request path; assertions compare the URLs the client built.
# Requires: bash, curl, jq, python3. Usage: resources/bin/test-orchestrator-client-tenant.sh

here="$(cd "$(dirname "$0")" && pwd)"
client="$here/orchestrator-client"
workdir="$(mktemp -d)"
log_a="$workdir/a.log"
log_b="$workdir/b.log"
failures=0
passes=0

cleanup() {
  [ -n "$server_pid" ] && kill "$server_pid" 2>/dev/null
  rm -rf "$workdir"
}
trap cleanup EXIT

# Fake orchestrator nodes. Node A is the Raft leader (leader-check 200), node B is a follower (leader-check 503).
cat > "$workdir/fake.py" <<'PY'
import json, sys
from http.server import BaseHTTPRequestHandler, HTTPServer
from threading import Thread

def make(logfile, leader):
    class H(BaseHTTPRequestHandler):
        def log_message(self, *a): pass
        def do_HEAD(self): self.send_response(200); self.end_headers()
        def do_GET(self):
            with open(logfile, "a") as f: f.write(self.path + "\n")
            p = self.path.split("?")[0]
            code = 200
            if p.endswith("/leader-check") and not leader: code = 503
            if p.endswith("/routed-leader-check") and not leader: code = 503
            msg, details = "ok", {"Key": {"Hostname": "h", "Port": 3306}, "Hostname": "orc-a"}
            if p.endswith("/raft-state"): msg = details = "Leader"
            if p.endswith("/tenants") and "/tenants/" not in p + "/":
                details = [{"ID": "t1", "Provider": "mysql"}]
            body = json.dumps({"Code": "OK", "Message": msg, "Details": details}).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
    return H

def serve(port, logfile, leader):
    HTTPServer(("127.0.0.1", port), make(logfile, leader)).serve_forever()

pa, pb, la, lb = int(sys.argv[1]), int(sys.argv[2]), sys.argv[3], sys.argv[4]
Thread(target=serve, args=(pb, lb, False), daemon=True).start()
serve(pa, la, True)
PY

port_a=$((20000 + RANDOM % 10000)); port_b=$((port_a + 1))
python3 "$workdir/fake.py" $port_a $port_b "$log_a" "$log_b" &
server_pid=$!
for i in $(seq 1 50); do curl -s -o /dev/null "http://127.0.0.1:$port_a/" && break; sleep 0.1; done

api_a="http://127.0.0.1:$port_a/api"
api_b="http://127.0.0.1:$port_b/api"

# run_case <name> <expected paths on node A, newline separated, leader-check excluded> <env/args...>
# env assignments go through "env", args after "--"
run_case() {
  local name="$1" expected="$2"; shift 2
  : > "$log_a"; : > "$log_b"
  env -u ORCHESTRATOR_TENANT "$@" > "$workdir/out" 2> "$workdir/err"
  local rc=$?
  local got
  got="$(cat "$log_a")"
  # leader detection probes (multi-URL setups) are asserted separately below
  [ -z "$KEEP_PROBES" ] && got="$(echo "$got" | grep -v -e '/leader-check$' -e '/routed-leader-check$')"
  if [ "$got" == "$expected" ]; then
    passes=$((passes+1)); echo "ok   - $name"
  else
    failures=$((failures+1)); echo "FAIL - $name (rc=$rc)"; echo "  expected: $(echo $expected)"; echo "  got:      $(echo $got)"; sed 's/^/  stderr: /' "$workdir/err"
  fi
}

C="$client"
# --- single API URL ---
run_case "no tenant, single api: unchanged" \
  "/api/discover/h/3306" \
  ORCHESTRATOR_API="$api_a" $C -c discover -i h:3306
run_case "no tenant, root URL without /api gets /api appended: unchanged" \
  "/api/discover/h/3306" \
  ORCHESTRATOR_API="http://127.0.0.1:$port_a" $C -c discover -i h:3306
run_case "tenant via -T" \
  "/api/tenants/t1/discover/h/3306" \
  ORCHESTRATOR_API="$api_a" $C -T t1 -c discover -i h:3306
run_case "tenant via --tenant" \
  "/api/tenants/t1/discover/h/3306" \
  ORCHESTRATOR_API="$api_a" $C --tenant t1 -c discover -i h:3306
run_case "tenant via -tenant" \
  "/api/tenants/t1/discover/h/3306" \
  ORCHESTRATOR_API="$api_a" $C -tenant t1 -c discover -i h:3306
run_case "tenant via env" \
  "/api/tenants/t-2/discover/h/3306" \
  ORCHESTRATOR_API="$api_a" ORCHESTRATOR_TENANT=t-2 $C -c discover -i h:3306
run_case "flag overrides env" \
  "/api/tenants/t1/discover/h/3306" \
  ORCHESTRATOR_API="$api_a" ORCHESTRATOR_TENANT=other $C -T t1 -c discover -i h:3306
run_case "-t is still the tag flag and does not set the tenant" \
  "/api/discover/h/3306" \
  ORCHESTRATOR_API="$api_a" $C -t sometag -c discover -i h:3306
run_case "query string preserved after tenant prefix" \
  "/api/tenants/t1/ack-all-recoveries?comment=why" \
  ORCHESTRATOR_API="$api_a" $C -T t1 -c api -P "ack-all-recoveries?comment=why"
run_case "-c api with leading slash, tenant" \
  "/api/tenants/t1/clusters" \
  ORCHESTRATOR_API="$api_a" $C -T t1 -c api -P /clusters
run_case "-c api with leading slash, no tenant: unchanged" \
  "/api//clusters" \
  ORCHESTRATOR_API="$api_a" $C -c api -P /clusters

# --- global endpoints never carry the tenant ---
for gp in all-tenants/clusters-info reload-configuration agents seeds health status lb-check _ping grab-election reelect metrics headers raft-state raft-peers raft-health "raft-yield-hint/node1" "leader-check/418"; do
  run_case "global endpoint $gp has no tenant" \
    "/api/$gp" \
    ORCHESTRATOR_API="$api_a" $C -T t1 -c api -P "$gp"
done
for gp in leader-check routed-leader-check; do
  KEEP_PROBES=1 run_case "global endpoint $gp has no tenant" \
    "/api/$gp" \
    ORCHESTRATOR_API="$api_a" $C -T t1 -c api -P "$gp"
done
run_case "tenants endpoint is global and listable (-c tenants)" \
  "/api/tenants" \
  ORCHESTRATOR_API="$api_a" $C -T t1 -c tenants
run_case "-c tenants without tenant" \
  "/api/tenants" \
  ORCHESTRATOR_API="$api_a" $C -c tenants
run_case "raft-leader-hostname (raft-state + status) global even with tenant" \
  "$(printf '/api/raft-state\n/api/status')" \
  ORCHESTRATOR_API="$api_a" $C -T t1 -c raft-leader-hostname
run_case "tenant-scoped path whose name merely starts like a global one" \
  "/api/tenants/t1/healthy-thing" \
  ORCHESTRATOR_API="$api_a" $C -T t1 -c api -P healthy-thing

# --- several URLs: leader detection must not be affected by the tenant ---
multi="$api_b $api_a"
run_case "multi api, no tenant: leader A found, call goes to A" \
  "/api/discover/h/3306" \
  ORCHESTRATOR_API="$multi" $C -c discover -i h:3306
run_case "multi api, tenant: leader A found, call goes to A under tenant" \
  "/api/tenants/t1/discover/h/3306" \
  ORCHESTRATOR_API="$multi" $C -T t1 -c discover -i h:3306
run_case "multi api, tenant via env" \
  "/api/tenants/t1/discover/h/3306" \
  ORCHESTRATOR_API="$multi" ORCHESTRATOR_TENANT=t1 $C -c discover -i h:3306

# leader-check itself must be untenanted on every probed node (including the follower)
: > "$log_a"; : > "$log_b"
ORCHESTRATOR_API="$multi" $C -T t1 -c discover -i h:3306 > /dev/null 2>&1
if [ "$(cat "$log_b")" == "/api/leader-check" ] && grep -qx "/api/leader-check" "$log_a" && ! grep -q "tenants/t1/.*leader-check" "$log_a" "$log_b"; then
  passes=$((passes+1)); echo "ok   - leader-check probes are never tenant-scoped (node B probed once, only leader-check)"
else
  failures=$((failures+1)); echo "FAIL - leader-check probes: A=[$(echo $(cat $log_a))] B=[$(echo $(cat $log_b))]"
fi

# --- validation ---
ORCHESTRATOR_API="$api_a" $C -T 'Bad/Tenant' -c discover -i h:3306 > /dev/null 2> "$workdir/err"
if [ $? -ne 0 ] && grep -q "invalid tenant" "$workdir/err"; then
  passes=$((passes+1)); echo "ok   - invalid tenant ID rejected"
else
  failures=$((failures+1)); echo "FAIL - invalid tenant ID not rejected"
fi

# --- help documents the flag and the command ---
help_out="$(ORCHESTRATOR_API="$api_a" $C -c help 2>&1)"
if echo "$help_out" | grep -q -e '--tenant' && echo "$help_out" | grep -q 'tenants'; then
  passes=$((passes+1)); echo "ok   - help documents --tenant and the tenants command"
else
  failures=$((failures+1)); echo "FAIL - help lacks tenant documentation"
fi

echo
echo "passed: $passes, failed: $failures"
[ $failures -eq 0 ]
