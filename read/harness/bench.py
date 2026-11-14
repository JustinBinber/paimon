#!/usr/bin/env python3
"""One explicitly chosen Linux Spark run; no automatic scale-up, kill, or retry."""
import argparse
import hashlib
import json
import os
import re
from pathlib import Path
import shutil
import subprocess
import sys
import time

GIB = 1024 ** 3


def dump(path, value):
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def sha(path):
    result = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            result.update(block)
    return result.hexdigest()


def inventory(root, output):
    """Hash all real seed files; a separate unmeasured I/O cost."""
    digest = hashlib.sha256()
    with output.open("x", encoding="utf-8") as stream:
        for path in sorted(root.rglob("*")):
            if path.is_symlink():
                raise ValueError("Seed/copy may not contain symlinks: " + str(path))
            if path.is_file():
                record = {"path": path.relative_to(root).as_posix(), "bytes": path.stat().st_size,
                          "sha256": sha(path)}
                line = json.dumps(record, sort_keys=True) + "\n"
                digest.update(line.encode())
                stream.write(line)
    return digest.hexdigest()


def numeric_file(path):
    try:
        value = path.read_text().strip()
        return value if value == "max" else int(value)
    except (OSError, ValueError):
        return None


def cgroup(pid):
    try:
        group = next(line.split(":", 2)[2] for line in Path(f"/proc/{pid}/cgroup").read_text().splitlines()
                     if line.startswith("0::"))
        base = Path("/sys/fs/cgroup") / group.lstrip("/")
        result = {"version": 2, "path": str(base)}
        for name in ("memory.current", "memory.max", "memory.swap.current", "memory.swap.max",
                     "memory.peak", "pids.max", "cpu.max"):
            value = numeric_file(base / name)
            if value is not None:
                result[name] = value
        events = base / "memory.events"
        if events.exists():
            result["memory.events"] = {a: int(b) for a, b in (line.split() for line in events.read_text().splitlines())}
        try:
            result["cpu.max"] = (base / "cpu.max").read_text().strip()
        except OSError:
            pass
        return result
    except (OSError, StopIteration, ValueError):
        return {"version": "unavailable"}


def memory():
    result = {}
    for line in Path("/proc/meminfo").read_text().splitlines():
        name, value = line.split(":", 1)
        if name in ("MemTotal", "MemAvailable", "SwapTotal", "SwapFree", "CommitLimit", "Committed_AS"):
            result[name] = int(value.split()[0]) * 1024
    vm = {}
    for line in Path("/proc/vmstat").read_text().splitlines():
        name, value = line.split()
        if name in ("pswpin", "pswpout", "oom_kill", "pgmajfault"):
            vm[name] = int(value)
    result["vmstat"] = vm
    return result


def reserve(planned_bytes):
    host, group = memory(), cgroup(os.getpid())
    effective_total, available = host["MemTotal"], host["MemAvailable"]
    if isinstance(group.get("memory.max"), int):
        effective_total = min(effective_total, group["memory.max"])
        available = min(available, group["memory.max"] - group.get("memory.current", 0))
    required_reserve = max(int(host["MemTotal"] * .20), int(3.2 * GIB))
    if available - planned_bytes < required_reserve:
        raise RuntimeError(f"Resource gate: available={available}, planned={planned_bytes}, reserve={required_reserve}")
    return {"host": host, "cgroup": group, "effectiveTotal": effective_total,
            "available": available, "plannedBytes": planned_bytes, "requiredReserveBytes": required_reserve}


def proc(pid):
    try:
        base = Path(f"/proc/{pid}")
        stat = (base / "stat").read_text()
        fields = stat[stat.rfind(")") + 2:].split()
        result = {"pid": pid, "ppid": int(fields[1]), "state": fields[0],
                  "utimeTicks": int(fields[11]), "stimeTicks": int(fields[12]),
                  "startTicks": int(fields[19]), "virtualBytes": int(fields[20]),
                  "rssBytes": int(fields[21]) * os.sysconf("SC_PAGE_SIZE")}
        for line in (base / "status").read_text().splitlines():
            name, value = line.split(":", 1)
            if name in ("VmRSS", "VmHWM", "RssAnon", "VmSwap", "VmPeak"):
                result[name] = int(value.split()[0]) * 1024
        try:
            result["io"] = {name: int(value) for name, value in
                            (line.split(": ") for line in (base / "io").read_text().splitlines())}
        except (OSError, ValueError):
            result["io"] = "unavailable"
        return result
    except (OSError, IndexError, ValueError):
        return None


def process_tree(root_pid):
    all_processes = [proc(int(p.name)) for p in Path("/proc").iterdir() if p.name.isdecimal()]
    all_processes = [p for p in all_processes if p]
    selected = {root_pid}
    while True:
        added = {p["pid"] for p in all_processes if p["ppid"] in selected} - selected
        if not added:
            break
        selected.update(added)
    return [p for p in all_processes if p["pid"] in selected]


def snapshots(warehouse):
    directory = warehouse / "bench.db" / "target" / "snapshot"
    return [{"name": p.name, "sha256": sha(p), "bytes": p.stat().st_size}
            for p in sorted(directory.glob("*")) if p.is_file()]


def event_metrics(directory):
    totals = {"taskAttempts": 0, "failedTaskAttempts": 0, "executorRunTimeMillis": 0,
              "jvmGCTimeMillis": 0, "memoryBytesSpilled": 0, "diskBytesSpilled": 0,
              "shuffleReadBytes": 0, "shuffleReadRecords": 0,
              "shuffleWriteBytes": 0, "shuffleWriteRecords": 0}
    errors = []
    for path in directory.rglob("*"):
        if not path.is_file() or path.name.startswith("."):
            continue
        try:
            with path.open(encoding="utf-8") as stream:
                for line in stream:
                    event = json.loads(line)
                    if event.get("Event") != "SparkListenerTaskEnd":
                        continue
                    totals["taskAttempts"] += 1
                    if event.get("Task End Reason", {}).get("Reason") != "Success":
                        totals["failedTaskAttempts"] += 1
                    metrics = event.get("Task Metrics", {})
                    for source, dest in (("Executor Run Time", "executorRunTimeMillis"),
                                         ("JVM GC Time", "jvmGCTimeMillis"),
                                         ("Memory Bytes Spilled", "memoryBytesSpilled"),
                                         ("Disk Bytes Spilled", "diskBytesSpilled")):
                        totals[dest] += metrics.get(source, 0)
                    read = metrics.get("Shuffle Read Metrics", {})
                    totals["shuffleReadBytes"] += read.get("Remote Bytes Read", 0) + read.get("Local Bytes Read", 0)
                    totals["shuffleReadRecords"] += read.get("Total Records Read", 0)
                    write = metrics.get("Shuffle Write Metrics", {})
                    totals["shuffleWriteBytes"] += write.get("Shuffle Bytes Written", 0)
                    totals["shuffleWriteRecords"] += write.get("Shuffle Records Written", 0)
        except (OSError, UnicodeError, ValueError) as error:
            errors.append({"file": str(path), "error": str(error)})
    return {"scope": "all completed task attempts including process gate, not just SQL", "totals": totals,
            "parseErrors": errors, "missingTaskMetrics": totals["taskAttempts"] == 0}


def run(args):
    if sys.platform != "linux":
        raise RuntimeError("This controller requires Linux /proc; do not silently substitute Windows metrics")
    root = args.root.resolve()
    root.mkdir(parents=True, exist_ok=True)
    marker = root / ".commit-memory-scratch"
    if not marker.exists():
        # User explicitly chooses scratch root. No deletion or cleanup is performed by this script.
        marker.write_text("Synthetic Paimon benchmark scratch; retain raw evidence.\n")
    if not args.label or any(c not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_" for c in args.label):
        raise ValueError("Use a plain unique run label")
    if args.files > 32 and not args.scale_authorized:
        raise RuntimeError("Parent must select this scale after semantic/observer gates: pass --scale-authorized")
    run_dir = root / "runs" / args.label
    run_dir.mkdir(parents=True, exist_ok=False)
    seed = root / "seed"
    seed_identity = root / "seed-identity.json"
    dump(run_dir / "request.json", {k: str(v) if isinstance(v, Path) else v for k, v in vars(args).items()})
    disk = shutil.disk_usage(root)
    if disk.free < args.min_disk_free_gib * GIB:
        raise RuntimeError("Insufficient scratch disk reserve")
    resource_gate = reserve(int(args.planned_rss_gib * GIB))
    dump(run_dir / "resource-gate.json", resource_gate)
    if not (args.spark_home / "jars" / "spark-core_2.12-3.5.9.jar").is_file():
        raise RuntimeError("Pinned Spark3.5.9 distribution required")
    provenance = json.loads(args.provenance.read_text(encoding="utf-8-sig"))
    jar_hash = sha(args.paimon_jar)
    if provenance.get("buildExitCode") != 0 or provenance.get("artifactSha256", "").lower() != jar_hash:
        raise RuntimeError("Completed build provenance does not match the actual Paimon artifact")
    shutil.copyfile(args.provenance, run_dir / "binary-provenance.json")
    dump(run_dir / "binary-hashes.json", {"paimon": jar_hash, "harness": sha(args.harness_jar),
                                         "sparkCore": sha(args.spark_home / "jars" / "spark-core_2.12-3.5.9.jar")})
    if args.phase == "fixture":
        if seed.exists() or seed_identity.exists():
            raise RuntimeError("Seed already exists; retain it and choose a fresh scratch root")
        warehouse = seed
    else:
        frozen = json.loads(seed_identity.read_text())
        if frozen["files"] != args.files or frozen["extraColumns"] != args.extra_columns:
            raise RuntimeError("Fixture dimensions differ from frozen seed")
        if frozen.get("splitTargetSize") != args.split_target_size or frozen.get("openFileCost") != args.open_file_cost:
            raise RuntimeError("Declared Paimon split settings differ from frozen seed")
        before = inventory(seed, run_dir / "seed-before.jsonl")
        if before != frozen["treeSha256"]:
            raise RuntimeError("Frozen seed changed")
        if args.phase in ("validate", "inspect"):
            if not args.measurement_label:
                raise RuntimeError("Validation/inspection requires --measurement-label")
            if any(c not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_" for c in args.measurement_label):
                raise ValueError("Invalid measurement label")
            warehouse = root / "runs" / args.measurement_label / "warehouse"
            if not warehouse.is_dir():
                raise RuntimeError("Measurement private warehouse does not exist")
        else:
            warehouse = run_dir / "warehouse"
            shutil.copytree(seed, warehouse, symlinks=False)
            copied = inventory(warehouse, run_dir / "copy-before.jsonl")
            if copied != frozen["treeSha256"]:
                raise RuntimeError("Independent private copy differs from seed")
    event_dir = run_dir / "events"
    event_dir.mkdir()
    temp_dir = run_dir / "spark-local"
    temp_dir.mkdir()
    dump(run_dir / "snapshots-before.json", snapshots(warehouse))
    command = [str(args.spark_home / "bin" / "spark-submit"), "--master", args.master,
               "--driver-memory", args.driver_memory, "--executor-memory", args.executor_memory,
               "--class", "bench.CommitMemoryBench", "--jars", str(args.paimon_jar.resolve())]
    conf = {"spark.local.dir": str(temp_dir), "spark.eventLog.enabled": "true",
            "spark.eventLog.dir": event_dir.as_uri(), "spark.eventLog.compress": "false",
            "spark.speculation": "false", "spark.task.maxFailures": "1", "spark.sql.shuffle.partitions": "2",
            "spark.dynamicAllocation.enabled": "false", "spark.executor.cores": "1",
            "spark.executor.memory": args.executor_memory,
            "spark.executor.memoryOverhead": "384m", "spark.rpc.message.maxSize": "128",
            "spark.driver.extraJavaOptions": "-XX:+UseG1GC -Xlog:gc*:file=" + str(run_dir / "driver-gc.log") + ":time,uptime,level,tags",
            "spark.executor.extraJavaOptions": "-XX:+UseG1GC -Xlog:gc*:file=" + str(run_dir / "executor-gc-%p.log") + ":time,uptime,level,tags"}
    if args.split_target_size:
        conf["bench.table.source.split.target-size"] = args.split_target_size
    if args.open_file_cost:
        conf["bench.table.source.split.open-file-cost"] = args.open_file_cost
    # Explicit factors only; do not sweep settings or retry failures automatically.
    for value in args.conf:
        key, setting = value.split("=", 1)
        if any(word in key.lower() for word in ("password", "secret", "token", "credential", "authorization")):
            raise ValueError("Credentials must never enter benchmark configuration/evidence")
        if key in ("spark.sql.files.maxRecordsPerFile", "spark.sql.paimon.target-file-size"):
            raise ValueError("Do not impose tiny-file output settings on MERGE")
        conf[key] = setting
    for key, value in conf.items():
        command += ["--conf", key + "=" + value]
    required = args.required_executors if args.phase in ("merge", "limit20", "read") else 0
    command += [str(args.harness_jar.resolve()), args.phase, str(warehouse), str(run_dir),
                str(args.files), str(args.tasks), str(args.extra_columns), str(required)]
    # Do not dump environment: it may contain cloud credentials or tokens.
    dump(run_dir / "command.json", command)
    state = {"state": "launching", "time": time.time(), "phase": args.phase, "label": args.label,
             "resourceGate": resource_gate}
    dump(run_dir / "state.json", state)
    peak_rss, min_available, observer_errors = 0, resource_gate["available"], []
    seen_processes = {}
    started = time.monotonic()
    with (run_dir / "stdout.raw.log").open("wb") as raw, (run_dir / "observer.jsonl").open("x") as observations:
        child = subprocess.Popen(command, stdout=raw, stderr=subprocess.STDOUT, start_new_session=True)
        child_identity = proc(child.pid)
        state.update(state="running", pid=child.pid, processGroupId=os.getpgid(child.pid),
                     processStartTicks=child_identity["startTicks"] if child_identity else None)
        dump(run_dir / "state.json", state)
        dump(root / "active-run.json", state)
        print(json.dumps({"state": "running", "pid": child.pid, "stateFile": str(run_dir / "state.json")}), flush=True)
        try:
            while True:
                try:
                    processes, host, group = process_tree(child.pid), memory(), cgroup(child.pid)
                    seen_processes.update({p["pid"]: p["startTicks"] for p in processes})
                    peak_rss = max(peak_rss, sum(p["rssBytes"] for p in processes))
                    available = host["MemAvailable"]
                    if isinstance(group.get("memory.max"), int):
                        available = min(available, group["memory.max"] - group.get("memory.current", 0))
                    min_available = min(min_available, available)
                    snapshot = {"time": time.time(), "processes": processes, "host": host, "cgroup": group,
                                "effectiveAvailableBytes": available}
                    observations.write(json.dumps(snapshot) + "\n")
                    observations.flush()
                except Exception as error:
                    observer_errors.append(str(error))
                code = child.poll()
                if code is not None:
                    break
                time.sleep(1)
        except KeyboardInterrupt:
            state.update(state="controller-interrupted-child-not-terminated", time=time.time())
            dump(run_dir / "state.json", state)
            dump(root / "active-run.json", state)
            print("Child left running; inspect state.json and the existing PID. Do not duplicate.", file=sys.stderr)
            raise
    lingering = [p for p in (proc(pid) for pid in seen_processes) if p and p["state"] != "Z"
                 and p["startTicks"] == seen_processes[p["pid"]]]
    state.update(state="driver-exited-descendants-live" if lingering else "exited", lingeringProcesses=lingering,
                 time=time.time(), exitCode=code, wallSeconds=time.monotonic() - started,
                 sampledPeakProcessTreeRssBytes=peak_rss, sampledMinHostAvailableBytes=min_available,
                 observerErrors=observer_errors)
    dump(run_dir / "state.json", state)
    dump(root / "active-run.json", state)
    dump(run_dir / "snapshots-after.json", snapshots(warehouse))
    events = event_metrics(event_dir)
    dump(run_dir / "spark-event-metrics.json", events)
    text = (run_dir / "stdout.raw.log").read_text(encoding="utf-8", errors="replace")
    markers = [line for line in text.splitlines() if line.startswith("BENCH ")]
    dump(run_dir / "bench-markers.json", markers)
    observed = [line for line in markers if "Identity=" in line]
    identity_hashes = [re.findall(r"#([a-f0-9]{64})", line) for line in observed]
    identity_ok = bool(identity_hashes) and all(len(values) == 2 and all(value == jar_hash for value in values)
                                               for values in identity_hashes)
    heap_files = len(list(run_dir.glob("heap-*.jsonl")))
    required_heap_files = 1 + required if required > 0 else 1
    summary = {"processExitCode": code, "identityGate": identity_ok, "observerErrors": observer_errors,
               "heapObserverFiles": heap_files, "requiredHeapObserverFiles": required_heap_files,
               "sampledReserveViolated": min_available < resource_gate["requiredReserveBytes"],
               "javaObserverFailure": "BENCH_OBSERVER_FAILURE" in text,
               "sqlCompleted": "sqlCompleted=true" in text, "readCompleted": "readCompleted=true" in text,
               "validationCompleted": "validationCompleted=true" in text,
               "correctness": "independent validation required" if args.phase == "merge" else "see phase markers",
               "phase": args.phase, "capacityClaim": False}
    summary["failureClassification"] = ("java-heap-oom-observed" if "OutOfMemoryError: Java heap space" in text else
        "java-other-oom-observed" if "OutOfMemoryError" in text else
        "sigkill-cause-unconfirmed" if code == -9 else "process-failure-see-raw-log" if code else "none")
    summary["eventObserverGate"] = not events["parseErrors"] and (args.phase == "inspect" or not events["missingTaskMetrics"])
    summary["lingeringProcesses"] = lingering
    summary["phaseCompletionGate"] = ("fixtureValidated=true" in text if args.phase == "fixture" else
        "sqlCompleted=true" in text if args.phase == "merge" else
        "validationCompleted=true" in text if args.phase == "validate" else
        "readCompleted=true" in text if args.phase in ("read", "limit20") else "BENCH snapshot=" in text)
    observer_ok = (not observer_errors and not summary["javaObserverFailure"] and heap_files >= required_heap_files
                   and not summary["sampledReserveViolated"] and not lingering and summary["eventObserverGate"])
    if args.phase == "fixture" and code == 0 and summary["phaseCompletionGate"] and identity_ok and observer_ok:
        digest = inventory(seed, run_dir / "seed-frozen.jsonl")
        dump(seed_identity, {"files": args.files, "extraColumns": args.extra_columns, "treeSha256": digest,
                             "generationRun": args.label, "binarySha256": jar_hash,
                             "splitTargetSize": args.split_target_size, "openFileCost": args.open_file_cost})
        summary["seedFrozen"] = True
    elif args.phase != "fixture":
        summary["seedUnchanged"] = inventory(seed, run_dir / "seed-after.jsonl") == frozen["treeSha256"]
    dump(run_dir / "summary.json", summary)
    print(json.dumps({"state": "exited", "exitCode": code, "summary": str(run_dir / "summary.json")}), flush=True)
    if (code or not identity_ok or observer_errors or summary["javaObserverFailure"]
            or heap_files < required_heap_files or summary["sampledReserveViolated"] or lingering
            or not summary["eventObserverGate"] or not summary["phaseCompletionGate"]
            or not summary.get("seedUnchanged", True)):
        return 1
    return 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--label", required=True)
    parser.add_argument("--phase", choices=("fixture", "merge", "validate", "inspect", "limit20", "read"), required=True)
    parser.add_argument("--files", type=int, required=True)
    parser.add_argument("--extra-columns", type=int, default=0)
    parser.add_argument("--tasks", type=int, default=8)
    parser.add_argument("--split-target-size")
    parser.add_argument("--open-file-cost")
    parser.add_argument("--spark-home", type=Path, required=True)
    parser.add_argument("--paimon-jar", type=Path, required=True)
    parser.add_argument("--harness-jar", type=Path, required=True)
    parser.add_argument("--provenance", type=Path, required=True)
    parser.add_argument("--master", default="local-cluster[2,1,512]")
    parser.add_argument("--driver-memory", default="1g")
    parser.add_argument("--executor-memory", default="512m")
    parser.add_argument("--required-executors", type=int, default=2)
    parser.add_argument("--planned-rss-gib", type=float, default=4.5)
    parser.add_argument("--min-disk-free-gib", type=float, default=20)
    parser.add_argument("--scale-authorized", action="store_true")
    parser.add_argument("--measurement-label")
    parser.add_argument("--conf", action="append", default=[])
    args = parser.parse_args()
    if args.files < 1 or args.tasks < 1 or args.extra_columns < 0 or args.planned_rss_gib <= 0:
        parser.error("Positive dimensions and a real RSS budget are required")
    if any(value and not re.fullmatch(r"[0-9]+(?:b|kb|mb|gb)", value, re.I)
           for value in (args.split_target_size, args.open_file_cost)):
        parser.error("Split sizes require an integer and b/kb/mb/gb suffix")
    if sys.platform != "linux":
        return run(args)  # Raises the documented platform error before any job is launched.
    import fcntl
    args.root.resolve().mkdir(parents=True, exist_ok=True)
    with (args.root.resolve() / "runner.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        active = args.root.resolve() / "active-run.json"
        if active.exists():
            previous = json.loads(active.read_text())
            candidates = [(previous.get("pid"), previous.get("processStartTicks"))]
            candidates += [(p["pid"], p["startTicks"]) for p in previous.get("lingeringProcesses", [])]
            for pid, start in candidates:
                current = proc(pid) if pid else None
                if current and current["state"] != "Z" and (start is None or current["startTicks"] == start):
                    raise RuntimeError("Previous run still has a live process: inspect active-run.json, do not duplicate")
        return run(args)


if __name__ == "__main__":
    sys.exit(main())
