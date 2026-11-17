#!/usr/bin/env python3
"""Concurrency test for the global quota pool.

Fires `threads` x `per_thread` requests at N gateway replicas (round-robin)
with a single shared X-Client-ID and a fixed-window limit L per minute, then
asserts that EXACTLY L requests were accepted cluster-wide — the strongest
evidence that all replicas count into one strongly-consistent quota pool.

usage: load.py <limit> <threads> <per_thread> <port1> [port2 ...]
"""
import concurrent.futures
import http.client
import itertools
import sys
import threading


def main():
    limit = int(sys.argv[1])
    threads = int(sys.argv[2])
    per_thread = int(sys.argv[3])
    ports = [int(p) for p in sys.argv[4:]]
    assert ports, "need at least one gateway port"

    total = threads * per_thread
    assert total > limit, "test is only meaningful when total > limit"

    counts = {"ok": 0, "limited": 0, "other": 0}
    missing_retry_after = 0
    lock = threading.Lock()
    counter = itertools.count()

    def worker(_):
        nonlocal missing_retry_after
        local = {"ok": 0, "limited": 0, "other": 0}
        for _ in range(per_thread):
            port = ports[next(counter) % len(ports)]
            conn = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
            conn.request("GET", "/load", headers={"X-Client-ID": "load-test"})
            resp = conn.getresponse()
            resp.read()
            if resp.status == 200:
                local["ok"] += 1
            elif resp.status == 429:
                if not resp.getheader("Retry-After"):
                    missing_retry_after += 1
                local["limited"] += 1
            else:
                local["other"] += 1
            conn.close()
        with lock:
            for k, v in local.items():
                counts[k] += v

    with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as pool:
        list(pool.map(worker, range(threads)))

    print(f"requests={total} limit={limit} "
          f"ok={counts['ok']} limited={counts['limited']} other={counts['other']}")
    failures = []
    if counts["ok"] != limit:
        failures.append(f"accepted {counts['ok']}, expected exactly {limit}")
    if counts["limited"] != total - limit:
        failures.append(f"429s={counts['limited']}, expected {total - limit}")
    if counts["other"] != 0:
        failures.append("unexpected non-200/429 responses")
    if missing_retry_after:
        failures.append(f"{missing_retry_after} x 429 without Retry-After")

    if failures:
        print("LOAD TEST FAILED:")
        for f in failures:
            print("  -", f)
        sys.exit(1)
    print("LOAD TEST PASSED: cluster-wide quota enforced exactly")


if __name__ == "__main__":
    main()
