#!/usr/bin/env python3
"""Tenant-command traffic generator of the chaos suite (host side, plain HTTP against the nodes' database-network IPs).

  chaos-traffic.py run --nodes IP[,IP...] --tenants id=host:port[,id=host:port...] --prefix P --log FILE
                       [--workers 4] [--max N] [--seconds S] [--stop-file F] [--untag-every 10]
  chaos-traffic.py expected --log FILE --tenant ID      # sorted "name|value" lines that MUST exist (accepted, not untagged)
  chaos-traffic.py stats --log FILE

Every request is a tenant command that goes through Raft: GET /api/tenants/<t>/tag/<host>/<port>/<name>/<value> or
/untag/<host>/<port>/<name> (the API of this orchestrator mutates with GET). A command counts as ACCEPTED only when the
HTTP answer is 200 with {"Code":"OK"}; a failed attempt is retried on the next node. A request that died after being
sent (timeout, connection reset) is AMBIGUOUS: it may or may not have been applied, so a node holding it is not an error
but a node missing an ACCEPTED one is.
"""
import argparse, http.client, json, os, random, sys, threading, time, urllib.parse


def one_request(node, path, timeout):
    """Returns (kind, http_status, body) kind in ok|rejected|ambiguous|unreached."""
    conn = http.client.HTTPConnection(node, 3000, timeout=timeout)
    try:
        try:
            conn.connect()
        except Exception:
            return "unreached", 0, ""
        try:
            conn.request("GET", path)
            r = conn.getresponse()
            body = r.read().decode("utf-8", "replace")
        except Exception as e:  # sent, no answer: unknown outcome
            return "ambiguous", 0, repr(e)
        if r.status == 200:
            try:
                if json.loads(body).get("Code") == "OK":
                    return "ok", 200, body[:120]
            except Exception:
                pass
        return "rejected", r.status, body[:200]
    finally:
        conn.close()


def run(a):
    nodes = a.nodes.split(",")
    tenants = []
    for t in a.tenants.split(","):
        tid, hp = t.split("=")
        host, port = hp.split(":")
        tenants.append((tid, host, port))
    counter = [0]
    lock = threading.Lock()
    out = open(a.log, "a", buffering=1)
    stop = threading.Event()
    t_end = time.time() + a.seconds if a.seconds else None

    def worker(wid):
        rnd = random.Random(a.seed * 1000 + wid)
        mine = []  # accepted tags of this worker, candidates for untag
        while not stop.is_set():
            if a.stop_file and os.path.exists(a.stop_file):
                break
            if t_end and time.time() > t_end:
                break
            with lock:
                i = counter[0]
                if a.max and i >= a.max:
                    break
                counter[0] += 1
            tid, host, port = tenants[i % len(tenants)]
            if mine and a.untag_every and i % a.untag_every == 0:
                op, tname, tval = "untag", mine.pop(rnd.randrange(len(mine)))[0], ""
                # the tenant of the tag decides the instance
                tid, host, port = next(x for x in tenants if tname.endswith("@" + x[0]))
                path = "/api/tenants/%s/untag/%s/%s/%s" % (tid, host, port, urllib.parse.quote(tname))
            else:
                op = "tag"
                tname = "%s-%07d@%s" % (a.prefix, i, tid)
                tval = "%s-%d" % (tid, i)
                path = "/api/tenants/%s/tag/%s/%s/%s/%s" % (tid, host, port, urllib.parse.quote(tname), urllib.parse.quote(tval))
            kind, status, body = "unreached", 0, ""
            order = nodes[:]
            rnd.shuffle(order)
            attempts = 0
            for node in order * 2:
                attempts += 1
                kind, status, body = one_request(node, path, a.timeout)
                if kind in ("ok", "ambiguous"):
                    break
                time.sleep(0.15)
            if kind == "ok" and op == "tag":
                mine.append((tname, tval))
            rec = {"i": i, "op": op, "tenant": tid, "name": tname, "value": tval, "kind": kind, "http": status, "node": node, "attempts": attempts, "t": round(time.time(), 3)}
            if kind not in ("ok",):
                rec["body"] = body
            out.write(json.dumps(rec) + "\n")
            if a.pace:
                time.sleep(a.pace)

    ths = [threading.Thread(target=worker, args=(w,), daemon=True) for w in range(a.workers)]
    for t in ths:
        t.start()
    try:
        for t in ths:
            while t.is_alive():
                t.join(0.5)
    except KeyboardInterrupt:
        stop.set()
    out.write(json.dumps({"summary": True, "issued": counter[0]}) + "\n")
    out.close()


def load(path):
    recs = []
    for line in open(path):
        line = line.strip()
        if not line:
            continue
        r = json.loads(line)
        if "summary" not in r:
            recs.append(r)
    return recs


def expected(a):
    recs = sorted(load(a.log), key=lambda r: r["t"])
    state = {}
    for r in recs:
        if r["tenant"] != a.tenant or r["kind"] != "ok":
            continue
        if r["op"] == "tag":
            state[r["name"]] = r["value"]
        elif r["op"] == "untag":
            state.pop(r["name"], None)
    for k in sorted(state):
        print("%s|%s" % (k, state[k]))


def stats(a):
    recs = load(a.log)
    c = {}
    for r in recs:
        k = (r["op"], r["kind"])
        c[k] = c.get(k, 0) + 1
    print(json.dumps({"total": len(recs), **{"%s_%s" % k: v for k, v in sorted(c.items())}}))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run")
    r.add_argument("--nodes", required=True); r.add_argument("--tenants", required=True); r.add_argument("--prefix", default="g")
    r.add_argument("--log", required=True); r.add_argument("--workers", type=int, default=4); r.add_argument("--max", type=int, default=0)
    r.add_argument("--seconds", type=float, default=0); r.add_argument("--stop-file", default=""); r.add_argument("--untag-every", type=int, default=10)
    r.add_argument("--timeout", type=float, default=8.0); r.add_argument("--pace", type=float, default=0.0); r.add_argument("--seed", type=int, default=1)
    e = sub.add_parser("expected"); e.add_argument("--log", required=True); e.add_argument("--tenant", required=True)
    s = sub.add_parser("stats"); s.add_argument("--log", required=True)
    a = p.parse_args()
    {"run": run, "expected": expected, "stats": stats}[a.cmd](a)
