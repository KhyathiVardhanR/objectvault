# ObjectVault — Distributed S3-Style Object Storage

A distributed object store in C++, modeled on Amazon S3. It has a REST API for buckets, objects, and multipart uploads, and spreads data across 3–5 storage nodes. Each object is split into chunks. Each chunk is placed on 3 nodes by consistent hashing, verified by SHA-256 on every write, read, and background scrub, and re-replicated automatically when a node dies or a copy goes bad. Object metadata lives in PostgreSQL behind a Redis cache. The gateway and every storage node export Prometheus metrics.

**Stack:** C++20 · Boost.Asio / Boost.Beast · gRPC / Protobuf · PostgreSQL (libpqxx) · Redis (hiredis) · OpenSSL · prometheus-cpp · Docker

## Architecture

```
 clients ──HTTP/REST──►  ov-gateway (Boost.Beast, worker thread pool)
                         │  split object → 4 MiB chunks, SHA-256 each
                         │  consistent-hash ring → 3 replica nodes per chunk
                         │  parallel gRPC writes, quorum W=2 of R=3
                         │  repair manager · heartbeats · /metrics
             ┌───────────┼─────────────┬──────────────┐
             ▼           ▼             ▼              ▼
        ov-storage   ov-storage    ov-storage   …  (3–5 nodes, gRPC)
        chunks on disk, SHA-256 header, atomic writes, scrubber, /metrics

        PostgreSQL: buckets, objects, chunks, replica locations, multipart state
        Redis:      cache-aside object metadata (invalidation-safe async fills)
```

**Gateway** (`src/gateway`)
- **REST API:** `PUT/GET/HEAD/DELETE /{bucket}/{key}`; bucket create, delete, and list; prefix listing with pagination; multipart uploads (start, upload part, complete, abort) with S3-style composite ETags.
- **Multithreaded:** an acceptor thread hands each connection to a pool of worker threads. A separate I/O pool writes every chunk to every replica in parallel, so large objects upload with full parallelism.
- **Placement:** a consistent-hash ring with 128 virtual nodes per storage node. A chunk goes to the first 3 distinct live nodes clockwise from `hash(chunk_id)`.
- **Writes:** succeed once 2 of 3 replicas acknowledge. Chunks that reach only 2 replicas are queued for repair.
- **Reads:** fetch all of an object's chunks in parallel, try replicas in random order, and verify each chunk's SHA-256. A bad or missing copy falls over to another replica and is queued for repair.

**Storage node** (`src/storage`)
- **Atomic, durable writes:** write to a temp file, `fsync`, rename, `fsync` the directory, then a durability barrier. On macOS, `fsync` doesn't flush the drive cache, so writers share one `F_FULLFSYNC` across the node (group commit).
- **Integrity:** the SHA-256 is checked on write (against the gateway's digest) and on every read. A rate-limited background **scrubber** re-verifies every chunk. Corrupt chunks are quarantined and reported to the gateway in the next heartbeat.

**Replica repair** (`src/gateway/repair.h`)
- A storage node is marked down after 3 missed heartbeats (500 ms apart).
- Every chunk that had a replica on the dead node is copied from a healthy replica to the next live node on the ring, and the metadata is updated.
- Corrupt replicas and quorum-only writes are handled through a repair queue.

**Metadata** (`src/gateway/metadata.cc`)
- **PostgreSQL** tables for buckets, objects, chunks, replicas, uploads, parts, and the repair queue. Indexes cover the hot paths: `(bucket_id, key text_pattern_ops)` for prefix listing, `chunks(object_id, seq)`, and `replicas(node)` for repair scans. The object lookup is a prepared statement.
- **Redis cache-aside:**
  - **Hit:** one `MGET` round trip.
  - **Miss:** queries PostgreSQL. The refill happens on a background thread with pipelined Lua compare-and-set, so a slow refill can never overwrite a newer invalidation, even with several gateways.

## Results

**Setup:** Apple M4 laptop (10 cores) on macOS. Every node is a separate process on one machine sharing one SSD, with replication factor R=3, write quorum W=2, and 4 MiB chunks. Writes are **fully durable**: each acknowledged chunk is flushed with `F_FULLFSYNC` on the replicas that acknowledged it. Reproduce with `scripts/bench.py`.

### Upload throughput

Aggregate upload throughput from closed-loop clients ([full tables](results/throughput.md)):

| Object size | 3 nodes (4 / 16 / 64 clients) | 5 nodes (4 / 16 / 64 clients) |
|---|---|---|
| 1 MiB | 91 / 148 / 162 MB/s | 150 / 278 / 351 MB/s |
| 8 MiB | 174 / 192 / 199 MB/s | 144 / 179 / 173 MB/s |
| 64 MiB | 155 / 122 MB/s (4 / 16 clients) | 134 / 113 MB/s (4 / 16 clients) |
| 64 KiB | 9 / 18 / 23 MB/s | 14 / 26 / 38 MB/s |

- **1 MiB–64 MiB objects reach 90–351 MB/s.**
- **Small 64 KiB objects reach 9–38 MB/s.** They're limited by durable flushes per second: every replica write waits for a drive-cache flush, and this SSD manages a few hundred per second in total.
- **Downloads reach 1.0–1.5 GB/s.**

### Replica recovery after a node failure

On a 5-node cluster holding about 2 GiB of objects, one storage node was SIGKILLed per trial ([raw](results/recovery.md)):

| Trial | Chunks re-replicated | Failure detected | Full 3-way redundancy restored |
|---:|---:|---:|---:|
| 1 | 422 | 1.41 s | 4.99 s |
| 2 | 14 | 1.16 s | 1.48 s |
| 3 | 189 | 1.59 s | 1.91 s |
| 4 | 788 | 1.62 s | 6.77 s |
| 5 | 2 | 1.55 s | 1.87 s |

Reads stayed correct throughout, served by the surviving replicas, and writes kept succeeding while the node was down.

### Metadata lookups: PostgreSQL vs. PostgreSQL + Redis

HEAD requests on 10,000 objects with 8 clients ([full tables, including a 1-chunk layout](results/metadata.md)). The benchmark controls the cache hit rate exactly. It warms a chosen fraction of keys, then sends each request either to a warm key or to a never-read key, which guarantees a miss. "Server-side lookup" is the gateway's own timing of the metadata step; "HEAD" is end-to-end latency at the client.

**Objects of 16 chunks** (64 KiB with 4 KiB chunks). PostgreSQL has to join the chunk and replica rows:

| Configuration | Observed hit rate | Server-side lookup | vs PostgreSQL only | Client HEAD mean | vs PostgreSQL only |
|---|---:|---:|---:|---:|---:|
| PostgreSQL only | – | 294 µs | – | 0.334 ms | – |
| + Redis cache | 100% | **141 µs** | **52% faster** | 0.171 ms | 49% faster |
| + Redis cache | 89% | 254 µs | 13% faster | 0.277 ms | 17% faster |
| + Redis cache | 26% | 441 µs | 50% slower | 0.481 ms | 44% slower |

**What this shows:**
- **On a hit:** Redis returns the whole object record (chunk list and replica locations) in one key read, about 2× faster than the multi-row PostgreSQL join.
- **On a miss:** cache-aside costs a Redis read *plus* the database query, so the cache only pays off at high hit rates. Here the break-even is around 85%.
- **1-chunk objects:** the PostgreSQL lookup is already a single in-memory index probe (123 µs), and Redis is at best 18% faster.
- **On a real network:** both stores would be on other hosts, adding a round trip to each, and the database would serve far more data than fits in its memory. Both shift the trade-off further toward the cache.

### Correctness tests

`tests/test_api.py` runs 17 end-to-end tests against a live 4-node cluster:
- Round trips at chunk-boundary object sizes
- Keys with slashes and unicode
- Overwrites and cache invalidation
- Prefix listing with pagination and literal `%`/`_` in prefixes
- Multipart uploads: out-of-order parts, re-uploaded parts, bad ETags, abort
- **Bit rot:** flips a byte in a replica on disk, then checks that reads stay correct and the scrubber plus repair restore 3 good copies
- **Node failure:** SIGKILLs a storage node, then checks that every object stays readable, writes continue, and redundancy is restored

## Running

```bash
brew install cmake boost grpc libpqxx hiredis prometheus-cpp nlohmann-json postgresql@16 redis pkgconf
cmake -S . -B build -DCMAKE_PREFIX_PATH="/opt/homebrew;/opt/homebrew/opt/libpq" && cmake --build build -j
scripts/cluster.sh start 5                      # Postgres, Redis, 5 storage nodes, gateway on :8080

curl -X PUT localhost:8080/photos
curl -X PUT --data-binary @cat.jpg -H 'Content-Type: image/jpeg' localhost:8080/photos/2026/cat.jpg
curl localhost:8080/photos/2026/cat.jpg -o out.jpg
curl -I localhost:8080/photos/2026/cat.jpg
curl 'localhost:8080/photos?prefix=2026/'
curl localhost:8080/_admin/status               # node health, under-replicated chunks

scripts/cluster.sh kill-node s2                 # SIGKILL a storage node and watch repair
python3 -m venv .venv && .venv/bin/pip install requests pytest
STORAGE_ARGS="--scrub-interval 1" scripts/cluster.sh start 4 && .venv/bin/pytest tests/
```

With Docker (5 storage nodes, PostgreSQL, Redis, and Prometheus on :9090):

```bash
docker compose up -d --build
```

### Prometheus metrics

| Metric | Meaning |
|---|---|
| `ov_http_requests_total{op,code}`, `ov_http_request_seconds{op}` | API throughput and latency per operation |
| `ov_http_bytes_total{dir}` | Upload and download bytes |
| `ov_metadata_lookup_seconds{source=cache\|database}`, `ov_metadata_cache_total{result}` | Lookup latency by source, and cache hit rate |
| `ov_storage_node_up{node}`, `ov_replication_health{kind}`, `ov_replication_total{event}` | Node health, under-replicated chunks, repairs |
| `ov_storage_usage{kind}`, `ov_storage_bytes_total`, `ov_storage_op_seconds{op}` | Per-node storage utilization and chunk I/O |
| `ov_storage_checksum_failures_total`, `ov_storage_scrub_passes_total` | Integrity verification |

## Limitations

- **Single gateway:** it holds no state of its own, so you could run several behind a load balancer, but repair should then run in only one of them (leader election is not implemented).
- **Orphan copies:** when a dead node rejoins, the chunk copies it held but that were already re-replicated elsewhere stay on its disk; they aren't garbage-collected.
- **Not S3-compatible:** no SigV4 authentication, and responses are JSON rather than XML.
- **Laptop benchmarks:** all of them ran on one machine, so every node shares one SSD and one CPU. Small-object throughput in particular would be much higher with one disk per node.
