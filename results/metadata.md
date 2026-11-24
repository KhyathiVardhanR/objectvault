
### Metadata lookups, 16-chunk objects (64 KiB, 4 KiB chunks): 10,000 objects, 6,000 HEAD requests, 8 clients

| Configuration | Cache hit rate (observed) | Server-side lookup (µs) | Lookup vs PostgreSQL-only | Client HEAD mean (ms) | Client p99 (ms) | HEAD vs PostgreSQL-only |
|---|---:|---:|---:|---:|---:|---:|
| PostgreSQL only | – | 294 | – | 0.334 | 0.504 | – |
| PostgreSQL + Redis | 0% | 510 | -74% | 0.560 | 0.930 | -68% |
| PostgreSQL + Redis | 26% | 441 | -50% | 0.481 | 0.859 | -44% |
| PostgreSQL + Redis | 49% | 995 | -239% | 1.049 | 27.912 | -215% |
| PostgreSQL + Redis | 73% | 668 | -128% | 0.913 | 9.122 | -174% |
| PostgreSQL + Redis | 89% | 254 | +13% | 0.277 | 1.309 | +17% |
| PostgreSQL + Redis | 100% | 141 | +52% | 0.171 | 0.280 | +49% |

### Metadata lookups, 1-chunk objects (1 B): 10,000 objects, 6,000 HEAD requests, 8 clients

| Configuration | Cache hit rate (observed) | Server-side lookup (µs) | Lookup vs PostgreSQL-only | Client HEAD mean (ms) | Client p99 (ms) | HEAD vs PostgreSQL-only |
|---|---:|---:|---:|---:|---:|---:|
| PostgreSQL only | – | 123 | – | 0.165 | 0.244 | – |
| PostgreSQL + Redis | 0% | 224 | -82% | 0.262 | 0.416 | -59% |
| PostgreSQL + Redis | 26% | 200 | -62% | 0.233 | 0.395 | -42% |
| PostgreSQL + Redis | 51% | 168 | -36% | 0.200 | 0.355 | -21% |
| PostgreSQL + Redis | 76% | 141 | -14% | 0.173 | 0.324 | -5% |
| PostgreSQL + Redis | 90% | 132 | -7% | 0.165 | 0.297 | +0% |
| PostgreSQL + Redis | 100% | 101 | +18% | 0.130 | 0.198 | +21% |
