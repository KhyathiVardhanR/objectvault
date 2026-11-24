
### Replica recovery after SIGKILL, 5 storage nodes, ~2.0 GiB of objects

| Trial | Killed | Chunks re-replicated | Detection (s) | Full redundancy restored (s) | Repair rate (MB/s) |
|---:|---|---:|---:|---:|---:|
| 1 | s2 | 422 | 1.41 | 4.99 | 495 |
| 2 | s3 | 14 | 1.16 | 1.48 | 184 |
| 3 | s4 | 189 | 1.59 | 1.91 | 2555 |
| 4 | s5 | 788 | 1.62 | 6.77 | 643 |
| 5 | s1 | 2 | 1.55 | 1.87 | 26 |
