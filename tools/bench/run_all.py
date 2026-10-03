#!/usr/bin/env python3
"""Runs every Phase 9 measurement and writes docs/results.csv.

    python3 tools/bench/run_all.py [--build build/rel] [--quick] [--out docs/results.csv]

Real experiments start real raftkvd processes on this machine (durable fsync unless stated)
and drive them with raftkvload. The 20% message-drop row comes from the simulator and is
labelled as such. --quick shortens every run, to check the tooling, not to report numbers.

Each throughput figure is the median of three runs. The CSV names the machine; the README's
results table is generated from it by tools/results_table.py.
"""

import argparse
import csv
import json
import os
import platform
import random
import re
import shutil
import signal
import statistics
import subprocess
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


class Cluster:
    """N raftkvd processes on localhost, each with its own data directory."""

    def __init__(self, build, n, extra_args=(), server_build=None):
        self.build = Path(build)
        self.server_build = Path(server_build or build)
        self.n = n
        self.work = Path(tempfile.mkdtemp(prefix="raftkv-bench-"))
        base = random.randint(20000, 31999 - n)   # below every OS's ephemeral port range
        self.cluster_file = self.work / "cluster.json"
        self.cluster_file.write_text(json.dumps({str(i): f"127.0.0.1:{base + i - 1}" for i in range(1, n + 1)}))
        self.extra_args = list(extra_args)
        self.procs = {}

    def start(self, i):
        log = open(self.work / f"node{i}.log", "ab")
        self.procs[i] = subprocess.Popen(
            [str(self.server_build / "raftkvd"), "--id", str(i), "--cluster", str(self.cluster_file),
             "--data-dir", str(self.work / f"data{i}"), *self.extra_args],
            stdout=log, stderr=log)

    def __enter__(self):
        for i in range(1, self.n + 1):
            self.start(i)
        deadline = time.time() + 20
        while time.time() < deadline:   # ready once a write succeeds
            if self.ctl("put", "ready", "1", timeout_ms=2000).returncode == 0:
                return self
        raise RuntimeError(f"cluster of {self.n} never became ready; logs in {self.work}")

    def __exit__(self, *exc):
        for p in self.procs.values():
            if p.poll() is None:
                p.kill()
            p.wait()
        shutil.rmtree(self.work, ignore_errors=True)

    def ctl(self, *args, timeout_ms=10000, verbose=False):
        cmd = [str(self.build / "raftkvctl"), "--cluster", str(self.cluster_file), "--timeout-ms", str(timeout_ms)]
        if verbose:
            cmd.append("--verbose")
        return subprocess.run(cmd + list(args), capture_output=True, text=True)

    def leader(self):
        r = self.ctl("get", "ready", verbose=True)
        m = re.search(r"served-by (\d+)", r.stderr)
        if not m:
            raise RuntimeError("could not find the leader: " + r.stderr)
        return int(m.group(1))

    def load(self, seconds, clients=32, write_pct=100, warmup_ms=1000, background=False):
        cmd = [str(self.build / "raftkvload"), "--cluster", str(self.cluster_file), "--clients", str(clients),
               "--seconds", str(seconds), "--warmup-ms", str(warmup_ms), "--write-pct", str(write_pct)]
        if background:
            return subprocess.Popen(cmd, stdout=subprocess.PIPE, text=True)
        r = subprocess.run(cmd, capture_output=True, text=True)
        if r.returncode != 0:
            raise RuntimeError("raftkvload failed: " + r.stderr)
        return json.loads(r.stdout)


class Results:
    def __init__(self):
        self.rows = []

    def add(self, experiment, variant, metric, value, unit, notes=""):
        self.rows.append([experiment, variant, metric, value if isinstance(value, str) else f"{value:.4g}", unit, notes])
        print(f"  {experiment:28} {variant:24} {metric:16} {self.rows[-1][3]:>10} {unit}", flush=True)

    def write(self, path):
        with open(path, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["experiment", "variant", "metric", "value", "unit", "notes"])
            w.writerows(self.rows)


def median_of(runs, key):
    return statistics.median(r[key] for r in runs)


def machine_rows(res):
    if platform.system() == "Darwin":
        sysctl = lambda k: subprocess.run(["sysctl", "-n", k], capture_output=True, text=True).stdout.strip()
        cpu = sysctl("machdep.cpu.brand_string")
        mem_gb = int(sysctl("hw.memsize") or 0) / 2**30
        os_name = "macOS " + platform.mac_ver()[0]
        disk_note = "fsync is F_FULLFSYNC"
    else:
        cpu = next((l.split(":", 1)[1].strip() for l in open("/proc/cpuinfo") if l.startswith("model name")), "?")
        mem_gb = os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES") / 2**30
        os_name = platform.platform()
        disk_note = "fsync"
    res.add("machine", "cpu", "model", cpu, "")
    res.add("machine", "cpu", "cores", str(os.cpu_count()), "")
    res.add("machine", "memory", "size", f"{mem_gb:.0f}", "GiB")
    res.add("machine", "os", "name", os_name, "", disk_note)
    res.add("machine", "date", "measured", time.strftime("%Y-%m-%d"), "")


def throughput(res, build, secs, reps, experiment, variant, n=3, extra=(), clients=32, write_pct=100,
               server_build=None, notes=""):
    runs = []
    for _ in range(reps):
        with Cluster(build, n, extra, server_build) as c:
            runs.append(c.load(secs, clients=clients, write_pct=write_pct))
    res.add(experiment, variant, "ops_per_sec", median_of(runs, "ops_per_sec"), "ops/s", notes)
    if write_pct > 0:
        res.add(experiment, variant, "write_p50", median_of(runs, "write_p50_ms"), "ms", notes)
        res.add(experiment, variant, "write_p99", median_of(runs, "write_p99_ms"), "ms", notes)
    if write_pct < 100:
        res.add(experiment, variant, "read_p50", median_of(runs, "read_p50_ms"), "ms", notes)
        res.add(experiment, variant, "read_p99", median_of(runs, "read_p99_ms"), "ms", notes)
    return median_of(runs, "ops_per_sec")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--build", default=str(ROOT / "build/rel"))
    ap.add_argument("--dev-build", default=str(ROOT / "build/dev"))
    ap.add_argument("--tsan-build", default=str(ROOT / "build/tsan"))
    ap.add_argument("--out", default=str(ROOT / "docs/results.csv"))
    ap.add_argument("--quick", action="store_true", help="short runs, to check the tooling only")
    args = ap.parse_args()

    secs, reps, kills = (2, 1, 1) if args.quick else (10, 3, 5)
    res = Results()
    machine_rows(res)

    print("1. throughput vs cluster size (writes, 32 clients)", flush=True)
    for n in (1, 3, 5):
        throughput(res, args.build, secs, reps, "cluster_size", f"{n} node{'s' if n > 1 else ''}", n=n)

    print("2. read and write latency (50% writes, 8 clients)", flush=True)
    throughput(res, args.build, secs, reps, "read_write_latency", "3 nodes, 50% writes", clients=8, write_pct=50)

    print("3. recovery after kill -9 of the leader", flush=True)
    gaps = []
    for _ in range(kills):
        with Cluster(args.build, 3) as c:
            leader = c.leader()
            load = c.load(secs, clients=4, background=True)
            time.sleep(1 + secs / 2)
            c.procs[leader].send_signal(signal.SIGKILL)
            out, _ = load.communicate(timeout=secs + 60)
            gaps.append(json.loads(out)["max_gap_ms"])
    res.add("leader_failover", f"3 nodes, {kills} kills", "median", statistics.median(gaps), "ms",
            "longest gap between completed writes")
    res.add("leader_failover", f"3 nodes, {kills} kills", "max", max(gaps), "ms")

    print("4. throughput with one follower stopped (stands in for a minority partition)", flush=True)
    runs = []
    for _ in range(reps):
        with Cluster(args.build, 3) as c:
            follower = next(i for i in (1, 2, 3) if i != c.leader())
            c.procs[follower].send_signal(signal.SIGSTOP)
            runs.append(c.load(secs))
            c.procs[follower].send_signal(signal.SIGCONT)
    res.add("minority_down", "3 nodes, 1 follower stopped", "ops_per_sec", median_of(runs, "ops_per_sec"), "ops/s",
            "SIGSTOP on one follower")

    print("5. batching: 1 entry per AppendEntries vs up to 64", flush=True)
    throughput(res, args.build, secs, reps, "batching", "max 1 entry per RPC", extra=["--max-batch", "1"])
    throughput(res, args.build, secs, reps, "batching", "max 64 entries per RPC", extra=["--max-batch", "64"])

    print("6. what fsync costs", flush=True)
    throughput(res, args.build, secs, reps, "fsync", "durable (fsync)")
    throughput(res, args.build, secs, reps, "fsync", "no fsync (unsafe)", extra=["--unsafe-no-fsync"],
               notes="survives a killed process, not a crashed machine")

    print("7. build type of the servers (same load generator)", flush=True)
    for name, b in (("release", args.build), ("asan+ubsan", args.dev_build), ("tsan", args.tsan_build)):
        if (Path(b) / "raftkvd").exists():
            throughput(res, args.build, secs, 1, "build_type", name, server_build=b)

    print("8. 20% message drops (simulator)", flush=True)
    r = subprocess.run([str(Path(args.build) / "raftkv_tests"), "--gtest_filter=FaultMatrix.TwentyPercentMessageDrops"],
                       capture_output=True, text=True)
    m = re.search(r"20% drops: (\d+) ops completed vs (\d+) with no loss \(([\d.]+)%\)", r.stdout)
    if m:
        res.add("message_drops", "20% drops, simulated", "throughput_kept", float(m.group(3)), "%",
                "simulator, virtual time; Phase 7 fault matrix")

    print("9. micro-benchmarks", flush=True)
    r = subprocess.run([str(Path(args.build) / "raftkv_bench"), "--benchmark_format=json",
                        "--benchmark_filter=BM_KvSnapshot|BM_SaveSnapshot|BM_AppendAndSync|BM_KvApplyPut",
                        *(["--benchmark_min_time=0.01s"] if args.quick else [])],
                       capture_output=True, text=True)
    for b in json.loads(r.stdout)["benchmarks"]:
        name = b["name"].split("/")
        unit = b["time_unit"]
        if name[0] == "BM_AppendAndSync":
            res.add("append_and_sync", "fsync" if name[1] == "1" else "no fsync", "per_entry", b["real_time"], unit)
        elif name[0] in ("BM_KvSnapshotTake", "BM_KvSnapshotRestore", "BM_SaveSnapshotDurably"):
            what = {"BM_KvSnapshotTake": "take", "BM_KvSnapshotRestore": "restore",
                    "BM_SaveSnapshotDurably": "save_durably"}[name[0]]
            res.add("snapshot_cost", f"{int(name[1]):,} keys", what, b["real_time"], unit)
        elif name[0] == "BM_KvApplyPut":
            res.add("state_machine", "apply put", "per_op", b["real_time"], unit)

    if args.quick:
        print("--quick: not writing results (the numbers are not meant to be reported)")
        return
    res.write(args.out)
    print(f"wrote {args.out}")


if __name__ == "__main__":
    sys.exit(main())
