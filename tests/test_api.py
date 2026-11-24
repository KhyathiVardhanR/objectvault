"""End-to-end tests against a running cluster (scripts/cluster.sh start 3).

Run: .venv/bin/pytest -v tests/
"""
import hashlib
import os
import pathlib
import subprocess
import time
import uuid

import pytest
import requests

GW = os.environ.get("OV_GATEWAY", "http://127.0.0.1:8080")
ROOT = pathlib.Path(__file__).resolve().parent.parent
DATA = pathlib.Path(os.environ.get("OV_DATA", ROOT / "data"))
CLUSTER = ROOT / "scripts" / "cluster.sh"
MiB = 1 << 20


def sha(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


@pytest.fixture
def bucket():
    name = "t-" + uuid.uuid4().hex[:12]
    assert requests.put(f"{GW}/{name}").status_code == 200
    yield name


def status():
    return requests.get(f"{GW}/_admin/status").json()


def wait_for(pred, timeout=30.0, every=0.1):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if pred():
            return True
        time.sleep(every)
    return False


# ---------------------------------------------------------------- buckets


def test_bucket_lifecycle():
    name = "b-" + uuid.uuid4().hex[:10]
    assert requests.put(f"{GW}/{name}").status_code == 200
    assert requests.put(f"{GW}/{name}").status_code == 409
    assert name in requests.get(f"{GW}/").json()["buckets"]
    requests.put(f"{GW}/{name}/k", data=b"x")
    assert requests.delete(f"{GW}/{name}").status_code == 409  # not empty
    requests.delete(f"{GW}/{name}/k")
    assert requests.delete(f"{GW}/{name}").status_code == 204
    assert requests.delete(f"{GW}/{name}").status_code == 404


def test_invalid_bucket_name():
    assert requests.put(f"{GW}/Bad_Name").status_code == 400


# ---------------------------------------------------------------- objects


@pytest.mark.parametrize("size", [0, 1, 4 * MiB - 1, 4 * MiB, 4 * MiB + 1, 13 * MiB + 7])
def test_put_get_roundtrip(bucket, size):
    body = os.urandom(size)
    r = requests.put(f"{GW}/{bucket}/obj", data=body, headers={"Content-Type": "application/x-test"})
    assert r.status_code == 200, r.text
    assert r.json()["etag"] == sha(body)
    g = requests.get(f"{GW}/{bucket}/obj")
    assert g.status_code == 200
    assert g.content == body
    assert g.headers["Content-Type"] == "application/x-test"
    h = requests.head(f"{GW}/{bucket}/obj")
    assert int(h.headers["Content-Length"]) == size
    assert h.headers["ETag"].strip('"') == sha(body)


def test_keys_with_slashes_and_unicode(bucket):
    key = "dir/sub dir/ñame%2F.txt"
    url = f"{GW}/{bucket}/" + requests.utils.quote(key)
    assert requests.put(url, data=b"hello").status_code == 200
    assert requests.get(url).content == b"hello"
    keys = [o["key"] for o in requests.get(f"{GW}/{bucket}", params={"prefix": "dir/"}).json()["objects"]]
    assert keys == [key]


def test_overwrite_and_cache_invalidation(bucket):
    url = f"{GW}/{bucket}/doc"
    requests.put(url, data=b"v1")
    assert requests.get(url).content == b"v1"  # now cached
    requests.put(url, data=b"version-2")
    assert requests.get(url).content == b"version-2"
    assert requests.head(url).headers["Content-Length"] == "9"


def test_delete(bucket):
    url = f"{GW}/{bucket}/gone"
    requests.put(url, data=b"bye")
    assert requests.delete(url).status_code == 204
    assert requests.get(url).status_code == 404
    assert requests.head(url).status_code == 404
    assert requests.delete(url).status_code == 404


def test_missing(bucket):
    assert requests.get(f"{GW}/{bucket}/nope").json()["error"] == "NoSuchKey"
    assert requests.put(f"{GW}/no-such-bucket-xyz/k", data=b"x").status_code == 404


def test_list_prefix_and_pagination(bucket):
    for i in range(25):
        requests.put(f"{GW}/{bucket}/logs/2026/{i:03d}", data=b"x")
    requests.put(f"{GW}/{bucket}/other", data=b"x")
    seen, after = [], ""
    while True:
        r = requests.get(f"{GW}/{bucket}", params={"prefix": "logs/", "max-keys": 10, "start-after": after}).json()
        seen += [o["key"] for o in r["objects"]]
        if not r["isTruncated"]:
            break
        after = r["nextStartAfter"]
    assert seen == [f"logs/2026/{i:03d}" for i in range(25)]
    # LIKE metacharacters in prefixes are literal
    requests.put(f"{GW}/{bucket}/a_b", data=b"x")
    requests.put(f"{GW}/{bucket}/axb", data=b"x")
    assert [o["key"] for o in requests.get(f"{GW}/{bucket}", params={"prefix": "a_"}).json()["objects"]] == ["a_b"]


# ---------------------------------------------------------------- multipart


def test_multipart_upload(bucket):
    url = f"{GW}/{bucket}/big.bin"
    uid = requests.post(url + "?uploads").json()["uploadId"]
    parts = [os.urandom(5 * MiB + 3), os.urandom(5 * MiB), os.urandom(1234)]
    etags = []
    # upload out of order, and re-upload part 2 (last write wins)
    for n in (3, 1, 2):
        r = requests.put(url, params={"uploadId": uid, "partNumber": n}, data=parts[n - 1])
        assert r.status_code == 200, r.text
    parts[1] = os.urandom(5 * MiB)
    requests.put(url, params={"uploadId": uid, "partNumber": 2}, data=parts[1])
    etags = [sha(p) for p in parts]
    body = {"parts": [{"partNumber": i + 1, "etag": e} for i, e in enumerate(etags)]}
    r = requests.post(url, params={"uploadId": uid}, json=body)
    assert r.status_code == 200, r.text
    assert r.json()["etag"].endswith("-3")
    full = b"".join(parts)
    g = requests.get(url)
    assert g.content == full
    assert int(requests.head(url).headers["Content-Length"]) == len(full)


def test_multipart_rejects_bad_parts(bucket):
    url = f"{GW}/{bucket}/m"
    uid = requests.post(url + "?uploads").json()["uploadId"]
    requests.put(url, params={"uploadId": uid, "partNumber": 1}, data=b"a")
    r = requests.post(url, params={"uploadId": uid}, json={"parts": [{"partNumber": 1, "etag": "wrong"}]})
    assert r.status_code == 400
    assert requests.delete(url, params={"uploadId": uid}).status_code == 204
    assert requests.put(url, params={"uploadId": uid, "partNumber": 1}, data=b"a").status_code == 404


# ---------------------------------------------------------------- integrity & failures


def chunk_files(chunk_ids):
    out = []
    for node_dir in DATA.glob("s*"):
        if not node_dir.is_dir():
            continue
        for cid in chunk_ids:
            p = node_dir / cid[:2] / cid
            if p.exists():
                out.append(p)
    return out


def object_chunk_ids(bucket, key):
    q = (
        "SELECT c.chunk_id FROM chunks c JOIN objects o ON o.id=c.object_id JOIN buckets b ON b.id=o.bucket_id "
        f"WHERE b.name='{bucket}' AND o.key='{key}' ORDER BY c.seq"
    )
    psql = subprocess.run(["brew", "--prefix", "postgresql@16"], capture_output=True, text=True).stdout.strip()
    r = subprocess.run(
        [f"{psql}/bin/psql", "-h", "/tmp", "-p", "5433", "-U", "postgres", "-d", "objectvault", "-tAc", q],
        capture_output=True, text=True, check=True,
    )
    return r.stdout.split()


def test_corrupted_replica_is_detected_and_repaired(bucket):
    body = os.urandom(6 * MiB)
    requests.put(f"{GW}/{bucket}/fragile", data=body)
    ids = object_chunk_ids(bucket, "fragile")
    files = chunk_files(ids[:1])
    assert len(files) == 3  # R=3 replicas of the first chunk
    # flip bytes in one replica's payload (bit rot)
    victim = files[0]
    raw = bytearray(victim.read_bytes())
    raw[100] ^= 0xFF
    victim.write_bytes(bytes(raw))
    # Reads stay correct regardless of which replica is tried first
    for _ in range(5):
        assert requests.get(f"{GW}/{bucket}/fragile").content == body
    # Force a read of every replica path until the corrupt copy is hit and repaired
    def repaired():
        requests.get(f"{GW}/{bucket}/fragile")
        good = [f for f in chunk_files(ids[:1]) if hashlib.sha256(f.read_bytes()[32:]).digest() == f.read_bytes()[:32]]
        return len(good) == 3 and status()["underReplicated"] == 0
    assert wait_for(repaired, timeout=20)


def test_node_failure_reads_survive_and_redundancy_restored(bucket):
    objs = {f"o{i}": os.urandom(2 * MiB + i) for i in range(10)}
    for k, v in objs.items():
        assert requests.put(f"{GW}/{bucket}/{k}", data=v).status_code == 200
    n_nodes = int((DATA / "nodes").read_text())
    if n_nodes < 4:
        pytest.skip("needs >= 4 storage nodes to restore R=3 after losing one")
    victim = "s2"
    subprocess.run([CLUSTER, "kill-node", victim], check=True)
    try:
        for k, v in objs.items():  # reads served by surviving replicas
            assert requests.get(f"{GW}/{bucket}/{k}").content == v
        assert wait_for(lambda: status()["nodes"][victim] == "down", timeout=10)
        assert wait_for(lambda: status()["underReplicated"] == 0, timeout=60)
        # every chunk is back to 3 live replicas
        for k, v in objs.items():
            assert requests.get(f"{GW}/{bucket}/{k}").content == v
        # writes keep working with a node down
        assert requests.put(f"{GW}/{bucket}/after", data=b"ok").status_code == 200
    finally:
        subprocess.run([CLUSTER, "start-node", victim], check=True)
        wait_for(lambda: status()["nodes"][victim] == "up", timeout=10)
