#!/usr/bin/env python3
"""Run the ObjectVault benchmark suite and write markdown results.

  scripts/bench.py throughput --nodes 3 5      upload/download MB/s matrix
  scripts/bench.py metadata                    Redis cache vs PostgreSQL-only lookups
  scripts/bench.py recovery --nodes 5 --gb 2   SIGKILL a node, time to full redundancy
"""
import argparse
import json
import os
import pathlib
import subprocess
import time
import urllib.request

ROOT = pathlib.Path(__file__).resolve().parent.parent
CLUSTER = ROOT / "scripts" / "cluster.sh"
BENCH = ROOT / "build" / "ov-bench"
RESULTS = ROOT / "results"
GW = "http://127.0.0.1:8080"


def sh(*args, env=None):
    return subprocess.run([str(a) for a in args], check=True, capture_output=True, text=True,
                          env={**os.environ, **(env or {})}).stdout


def cluster(n, gateway_args="", storage_args=""):
    sh(CLUSTER, "wipe")
    sh(CLUSTER, "start", n, env={"GATEWAY_ARGS": gateway_args, "STORAGE_ARGS": storage_args})


def bench(*args):
    out = sh(BENCH, *args, "--json")
    return json.loads(out.strip().splitlines()[-1])


def status():
    with urllib.request.urlopen(GW + "/_admin/status") as r:
        return json.load(r)


def throughput(nodes, duration):
    sizes = ["64K", "1M", "8M", "64M"]
    concs = [4, 16, 64]
    lines = []
    for n in nodes:
        cluster(n)
        lines += [f"\n### Upload, {n} storage nodes (R=3, W=2, 4 MiB chunks, durable writes)\n",
                  "| Object size | Clients | Throughput (MB/s) | Objects/s | p50 (ms) | p99 (ms) |",
                  "|---|---:|---:|---:|---:|---:|"]
        for s in sizes:
            for c in concs:
                if s == "64M" and c == 64:
                    continue  # 4 GiB in flight; not meaningful on one laptop
                r = bench("upload", "--size", s, "--concurrency", c, "--duration", duration)
                print(f"n={n} upload {s} c={c}: {r['MBps']:.1f} MB/s", flush=True)
                lines.append(f"| {s} | {c} | {r['MBps']:.1f} | {r['ops_per_sec']:.1f} | {r['p50_ms']:.1f} | {r['p99_ms']:.1f} |")
        lines += [f"\n### Download, {n} storage nodes\n",
                  "| Object size | Clients | Throughput (MB/s) | p50 (ms) | p99 (ms) |", "|---|---:|---:|---:|---:|"]
        for s in ["1M", "8M"]:
            for c in [4, 16]:
                r = bench("download", "--size", s, "--concurrency", c, "--duration", duration)
                print(f"n={n} download {s} c={c}: {r['MBps']:.1f} MB/s", flush=True)
                lines.append(f"| {s} | {c} | {r['MBps']:.1f} | {r['p50_ms']:.1f} | {r['p99_ms']:.1f} |")
    return "\n".join(lines) + "\n"


def lookup_stats():
    """Server-side metadata lookup (count, sum seconds) per source from the gateway's histograms."""
    out = {"cache": [0.0, 0.0], "database": [0.0, 0.0]}
    with urllib.request.urlopen("http://127.0.0.1:9200/metrics") as r:
        for line in r.read().decode().splitlines():
            for src in out:
                if line.startswith("ov_metadata_lookup_seconds_count") and f'source="{src}"' in line:
                    out[src][0] = float(line.rsplit(" ", 1)[1])
                if line.startswith("ov_metadata_lookup_seconds_sum") and f'source="{src}"' in line:
                    out[src][1] = float(line.rsplit(" ", 1)[1])
    return out


def head_run(objects, requests, conc, hit_rate=None):
    before = lookup_stats()
    args = ["head", "--objects", objects, "--requests", requests, "--concurrency", conc]
    if hit_rate is not None:
        args += ["--hit-rate", hit_rate]
    r = bench(*args)
    after = lookup_stats()
    # warm-up HEADs (cache fill phase) are excluded: subtract the requests' share only
    n = sum(after[s][0] - before[s][0] for s in after)
    secs = sum(after[s][1] - before[s][1] for s in after)
    warm = n - (r["ops"] + r["errors"])
    if warm > 0:  # warm-up lookups were all database misses
        db_n = after["database"][0] - before["database"][0]
        db_s = after["database"][1] - before["database"][1]
        per_miss = db_s / db_n if db_n else 0
        secs -= warm * per_miss
        n -= warm
    r["server_lookup_us"] = secs / n * 1e6 if n else 0.0
    cache_n = after["cache"][0] - before["cache"][0]
    r["server_hit_rate"] = cache_n / n if n else 0.0
    return r


def metadata_layout(name, chunk_size, obj_size, objects, requests, conc):
    extra = f"--chunk-size {chunk_size}"
    # Background scrubbing is off here: it re-hashes every chunk on disk and
    # would compete for CPU with the metadata path being measured.
    quiet = "--scrub-interval 0"
    cluster(3, f"--no-cache {extra}", quiet)
    bench("populate", "--objects", objects, "--concurrency", 32, "--size", obj_size)
    base = head_run(objects, requests, conc)
    print(f"[{name}] postgres-only: client {base['mean_ms']:.3f}ms, server {base['server_lookup_us']:.0f}us", flush=True)
    sh(CLUSTER, "stop")
    sh(CLUSTER, "start", 3, env={"GATEWAY_ARGS": extra, "STORAGE_ARGS": quiet})
    rows = []
    for hr in [0.0, 0.25, 0.5, 0.75, 0.9, 1.0]:
        r = head_run(objects, requests, conc, hr)
        rows.append(r)
        print(f"[{name}] redis hit={r['hit_rate']:.2f}: client {r['mean_ms']:.3f}ms, server {r['server_lookup_us']:.0f}us",
              flush=True)
    lines = [f"\n### Metadata lookups, {name}: {objects:,} objects, {requests:,} HEAD requests, {conc} clients\n",
             "| Configuration | Cache hit rate (observed) | Server-side lookup (µs) | Lookup vs PostgreSQL-only | "
             "Client HEAD mean (ms) | Client p99 (ms) | HEAD vs PostgreSQL-only |",
             "|---|---:|---:|---:|---:|---:|---:|",
             f"| PostgreSQL only | – | {base['server_lookup_us']:.0f} | – | {base['mean_ms']:.3f} | {base['p99_ms']:.3f} | – |"]
    for r in rows:
        lk = (1 - r["server_lookup_us"] / base["server_lookup_us"]) * 100
        cl = (1 - r["mean_ms"] / base["mean_ms"]) * 100
        lines.append(f"| PostgreSQL + Redis | {r['server_hit_rate']*100:.0f}% | {r['server_lookup_us']:.0f} | {lk:+.0f}% | "
                     f"{r['mean_ms']:.3f} | {r['p99_ms']:.3f} | {cl:+.0f}% |")
    return "\n".join(lines) + "\n"


def metadata(objects, requests, conc):
    out = metadata_layout("16-chunk objects (64 KiB, 4 KiB chunks)", "4K", "64K", objects, requests, conc)
    out += metadata_layout("1-chunk objects (1 B)", "4M", "1", objects, requests, conc)
    return out


def recovery(nodes, gb, trials):
    lines = [f"\n### Replica recovery after SIGKILL, {nodes} storage nodes, ~{gb} GiB of objects\n",
             "| Trial | Killed | Chunks re-replicated | Detection (s) | Full redundancy restored (s) | Repair rate (MB/s) |",
             "|---:|---|---:|---:|---:|---:|"]
    cluster(nodes)
    # ~gb GiB of distinct 8 MiB objects (16 writers x 64 slots = up to 8 GiB distinct)
    r = bench("upload", "--size", "8M", "--concurrency", 16, "--duration", max(3.0, gb * 1024 / 200),
              "--bucket", "recovery")
    print(f"loaded {r['ops'] * 8 / 1024:.2f} GiB", flush=True)
    for trial in range(1, trials + 1):
        victim = f"s{(trial % nodes) + 1}"
        before = metrics_value("ov_replication_total", 'event="bytes_repaired"')
        chunks_before = metrics_value("ov_replication_total", 'event="chunk_repaired"')
        t0 = time.time()
        sh(CLUSTER, "kill-node", victim)
        while status()["nodes"][victim] != "down":
            time.sleep(0.05)
        detect = time.time() - t0
        while not (status()["underReplicated"] == 0 and status()["lastRecoverySeconds"] > 0 and time.time() - t0 > detect + 0.3):
            time.sleep(0.05)
        restored = time.time() - t0
        moved = metrics_value("ov_replication_total", 'event="bytes_repaired"') - before
        chunks = metrics_value("ov_replication_total", 'event="chunk_repaired"') - chunks_before
        rate = moved / 1e6 / max(1e-9, restored - detect)
        print(f"trial {trial}: killed {victim}, {chunks:.0f} chunks, detect {detect:.2f}s, restored {restored:.2f}s", flush=True)
        lines.append(f"| {trial} | {victim} | {chunks:.0f} | {detect:.2f} | {restored:.2f} | {rate:.0f} |")
        sh(CLUSTER, "start-node", victim)
        while status()["nodes"][victim] != "up":
            time.sleep(0.1)
        time.sleep(1)
    return "\n".join(lines) + "\n"


def metrics_value(name, label):
    with urllib.request.urlopen("http://127.0.0.1:9200/metrics") as r:
        for line in r.read().decode().splitlines():
            if line.startswith(name) and label in line:
                return float(line.rsplit(" ", 1)[1])
    return 0.0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("suite", choices=["throughput", "metadata", "recovery", "all"])
    ap.add_argument("--nodes", type=int, nargs="+", default=[3, 5])
    ap.add_argument("--duration", default="6")
    ap.add_argument("--objects", type=int, default=20000)
    ap.add_argument("--requests", type=int, default=10000)
    ap.add_argument("--concurrency", type=int, default=16)
    ap.add_argument("--gb", type=float, default=2)
    ap.add_argument("--trials", type=int, default=5)
    a = ap.parse_args()
    RESULTS.mkdir(exist_ok=True)
    try:
        if a.suite in ("throughput", "all"):
            (RESULTS / "throughput.md").write_text(throughput(a.nodes, a.duration))
        if a.suite in ("metadata", "all"):
            (RESULTS / "metadata.md").write_text(metadata(a.objects, a.requests, a.concurrency))
        if a.suite in ("recovery", "all"):
            (RESULTS / "recovery.md").write_text(recovery(max(a.nodes), a.gb, a.trials))
    finally:
        sh(CLUSTER, "stop")


if __name__ == "__main__":
    main()
