"""Print a launch plan by default; --execute runs exactly one fresh application."""
import argparse
import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

from spec import (DISK_FLOOR, MEMORY_FLOOR, METRIC_AVAILABILITY, MODES, NARROW_OPTION, ORIGINAL_CLOUD_SEED, VARIANTS,
                  budget, case, dump, inventory, require, schedule, sha)

HERE = Path(__file__).resolve().parent


def arguments():
    p = argparse.ArgumentParser(__doc__)
    p.add_argument('--phase', choices=('plan', 'matrix', 'fixture', 'prepare', 'measure', 'validate'), default='plan')
    p.add_argument('--profile', choices=('local', 'cloud'), default='local')
    p.add_argument('--root', type=Path, required=True)
    p.add_argument('--seed', type=Path, help='warehouse containing bench.db/target, never a prepared source')
    p.add_argument('--name', default='fixture')
    p.add_argument('--variant', choices=VARIANTS, default='insert17')
    p.add_argument('--mode', choices=tuple(MODES), default='F')
    p.add_argument('--spark', type=Path)
    p.add_argument('--java-home', type=Path)
    p.add_argument('--python', default=sys.executable)
    p.add_argument('--sdk', type=Path)
    p.add_argument('--overlay', type=Path, help='optional production command overlay; identical for F/R/N')
    p.add_argument('--helpers', type=Path, help='jar built from the two Java helpers in this directory')
    p.add_argument('--fixture-jar', type=Path, help='existing CommitMemoryBench jar; needed only for fixture')
    p.add_argument('--budget-proof', type=Path, help='passing current-SDK local 2048-file N pilot calibration; required for cloud measure')
    p.add_argument('--narrow-option', default=NARROW_OPTION, help='confirmed production table property')
    p.add_argument('--execute', action='store_true')
    return p.parse_args()


def bind(a):
    paths = dict(sdk=a.sdk, overlay=a.overlay, helpers=a.helpers)
    binding = {key: dict(path=str(p.resolve()), sha256=sha(p)) for key, p in paths.items() if p is not None}
    binding['harness'] = {p.name: sha(p) for p in (HERE / 'experiment.py', HERE / 'spec.py', HERE / 'run.py')}
    binding['calibrationSHA256'] = sha(a.budget_proof) if a.budget_proof else None
    helper_binding = json.loads(a.helpers.with_suffix('.binding.json').read_text())
    require(helper_binding['sdkSHA256'] == sha(a.sdk) and helper_binding['helpersSHA256'] == sha(a.helpers),
            'helpers must be compiled against this exact final SDK')
    require(helper_binding['catalog'] == 'bench-counted', 'counting catalog helper missing')
    path = a.root / 'binary-binding.json'
    if path.exists():
        require(json.loads(path.read_text()) == binding, 'binary/harness changed; use a new run root')
    else:
        dump(path, binding)
    return binding


def clone_target(seed, warehouse):
    target = seed / 'bench.db/target'
    require(not warehouse.exists(), 'refuse to replace an existing warehouse')
    require(target.is_dir() and not target.is_symlink(), 'missing frozen target')
    (warehouse / 'bench.db').mkdir(parents=True)

    def copy(src, dst):
        p = Path(src)
        require(not p.is_symlink(), 'symlink in seed')
        if p.suffix == '.parquet':
            os.link(src, dst)  # immutable data only; copied metadata may be changed by ALTER/commit
        else:
            shutil.copy2(src, dst)
        return dst

    shutil.copytree(str(target), str(warehouse / 'bench.db/target'), copy_function=copy)


def process_samples(pid, psutil):
    """Process identity plus cumulative counters; never collect command lines/env."""
    rows, procs = [], []
    try:
        procs = [psutil.Process(pid)] + psutil.Process(pid).children(recursive=True)
    except psutil.Error:
        return rows, procs
    for p in procs:
        try:
            cpu, counters = p.cpu_times(), p.io_counters()
            row = dict(pid=p.pid, parentPID=p.ppid(), startEpoch=p.create_time(),
                       rss=p.memory_info().rss, cpuSeconds=cpu.user + cpu.system,
                       read_bytes=counters.read_bytes, write_bytes=counters.write_bytes)
            stat = Path('/proc') / str(p.pid) / 'stat'
            if stat.is_file():
                # Linux start ticks distinguish reused PIDs, matching old controller.processes.
                row['startTicks'] = int(stat.read_text().rsplit(') ', 1)[1].split()[19])
            rows.append(row)
        except (psutil.Error, OSError, IndexError):
            continue
    return rows, procs


def stop_tree(process, procs, psutil):
    for p in reversed(procs):
        try:
            p.terminate()
        except psutil.Error:
            pass
    _, alive = psutil.wait_procs(procs, timeout=5)
    for p in alive:
        try:
            p.kill()
        except psutil.Error:
            pass
    if process.poll() is None:
        process.kill()
    process.wait(timeout=15)


def calibrated_bound(a):
    if a.budget_proof is None:
        return None
    proof = json.loads(a.budget_proof.read_text())
    require(proof['sdkSHA256'] == sha(a.sdk) and proof['helpersSHA256'] == sha(a.helpers), 'budget pilot used a different binary')
    require(proof['pilotFiles'] == 2048 and proof['rowsPerFile'] == 2 and
            proof['columns'] == 259 and proof['updateColumns'] == 128 and proof['includesFooter'], 'pilot dimensions/footers incomplete')
    from spec import UPDATE_NAMES
    require(proof['writeCols'] == UPDATE_NAMES and proof['sourceProfile'] == case('local', 'insert17'), 'pilot SQL/source/write-column profile differs')
    require(proof['overlaySHA256'] == (sha(a.overlay) if a.overlay else None), 'pilot overlay differs')
    observed = proof['observedMaxPartialFileBytes']
    require(observed > 0 and proof['upperPartialFileBytes'] == (((observed * 5 + 3) // 4 + 4095) // 4096) * 4096,
            'bound must be round_up(maxObservedPartialFileBytes * 1.25,4096)')
    require(proof['probeEvidence'], 'missing pilot evidence')
    for leaf in proof['probeEvidence']:
        path = Path(leaf['path'])
        if not path.is_absolute():
            path = a.budget_proof.parent / path
        require(sha(path) == leaf['sha256'], 'pilot evidence digest mismatch')
    return proof['upperPartialFileBytes']


def source_io_summary(out, phase):
    streams = total_bytes = reads = seeks = positional_reads = overlap_streams = overlap_bytes = 0
    pids = set()
    if phase == 'measure':
        result = json.loads((out / 'sql-result.json').read_text())
        start, end = result['sqlStartEpoch'] * 1000, result['sqlEndEpoch'] * 1000
    for path in (out / (phase + '-source-io')).glob('source-io-*.jsonl'):
        with path.open(encoding='utf-8') as source:
            for line in source:
                r = json.loads(line)
                require(r['closeSucceeded'], 'source FileIO stream close failed')
                streams += 1
                total_bytes += r['bytes']
                reads += r['reads']
                seeks += r['seeks']
                positional_reads += r['positionalReads']
                pids.add(r['pid'])
                if phase == 'measure' and r['openEpochMillis'] <= end and r['epochMillis'] >= start:
                    overlap_streams += 1
                    overlap_bytes += r['bytes']
    require(streams, 'source FileIO counters missing; stop before another launch')
    summary = dict(streams=streams, bytes=total_bytes, reads=reads, seeks=seeks,
                   positionalReads=positional_reads, pids=sorted(pids),
                   meaning='source Parquet bytes returned by FileIO, including repeated/footer reads; not physical disk bytes')
    require(summary['bytes'] > 0, 'no real source FileIO bytes were observed')
    if phase == 'measure':
        summary['sqlOverlappingStreams'] = overlap_streams
        summary['sqlOverlappingBytes'] = overlap_bytes
        require(overlap_streams, 'source counters did not overlap timed SQL')
    dump(out / (phase + '-source-io-summary.json'), summary)


def main():
    a = arguments()
    if a.phase == 'matrix':
        require(a.profile == 'local', 'cloud sequencing belongs to the main coordinator')
        rows = schedule('local')
        if not a.execute:
            print(json.dumps(rows, indent=2))
            return
        # Keep the single-application launcher as the only resource/binary admission path.
        for row in rows:
            base = [sys.executable, str(HERE / 'run.py'), '--root', str(a.root), '--profile', 'local',
                    '--name', row['name'], '--variant', row['variant'], '--mode', row['mode'], '--execute']
            for key in ('seed', 'spark', 'java_home', 'sdk', 'overlay', 'helpers', 'fixture_jar', 'budget_proof', 'narrow_option', 'python'):
                value = getattr(a, key)
                if value is not None:
                    base += ['--' + key.replace('_', '-'), str(value)]
            for phase in ('prepare', 'measure', 'validate'):
                subprocess.run(base + ['--phase', phase], check=True)
            subprocess.run([sys.executable, str(HERE / 'archive.py'), 'freeze', '--root', str(a.root), '--name', row['name']], check=True)
            subprocess.run([sys.executable, str(HERE / 'follow.py'), '--root', str(a.root), '--name', row['name']], check=True)
            subprocess.run([sys.executable, str(HERE / 'archive.py'), 'retire', '--root', str(a.root), '--name', row['name']], check=True)
        return
    if a.phase == 'plan':
        require(not a.execute, 'plan never launches workloads')
        bound = calibrated_bound(a)
        print(json.dumps(dict(profile=a.profile, rounds=['F/R/N', 'R/N/F', 'N/F/R'],
                              cells=schedule(a.profile), metricAvailability=METRIC_AVAILABILITY,
                              budgets=[budget(case(a.profile, v), 'measure', partial_file_upper=bound) for v in VARIANTS]), indent=2))
        return
    require(a.name and Path(a.name).name == a.name and a.name not in ('.', '..'), 'unsafe name')
    require(a.narrow_option and a.narrow_option.startswith('data-evolution.') and
            all(ch.isalnum() or ch in '.-' for ch in a.narrow_option), 'supply the confirmed production table option')
    for label in ('spark', 'java_home', 'sdk', 'helpers'):
        require(getattr(a, label) is not None, '--' + label.replace('_', '-') + ' is required')
    require(a.phase != 'fixture' or (a.profile == 'local' and a.fixture_jar), 'fixture requires local profile and fixture jar')
    a.root = a.root.resolve()
    seed = (a.seed or (Path(ORIGINAL_CLOUD_SEED) if a.profile == 'cloud' else a.root / 'fixture/warehouse')).resolve()
    out = a.root / a.name
    require(not out.is_symlink(), 'linked output')
    c = case(a.profile, a.variant)
    jars = [p.resolve() for p in (a.overlay, a.helpers, a.sdk, a.fixture_jar) if p is not None]
    cp = os.pathsep.join(map(str, jars))
    submit = a.spark.resolve() / 'bin' / ('spark-submit.cmd' if os.name == 'nt' else 'spark-submit')
    cmd = [str(submit), '--master', 'local[2]' if a.profile == 'local' else 'local-cluster[2,1,1024]',
           '--driver-memory', '1g' if a.profile == 'local' else '4g', '--executor-memory', '1g',
           '--driver-class-path', cp, '--conf', 'spark.executor.extraClassPath=' + cp]
    configs = {'spark.executor.instances': '2', 'spark.executor.cores': '1',
               'spark.dynamicAllocation.enabled': 'false', 'spark.speculation': 'false',
               'spark.task.maxFailures': '1', 'spark.ui.enabled': 'false',
               'spark.sql.shuffle.partitions': '200', 'spark.sql.adaptive.enabled': 'true',
               'spark.sql.adaptive.coalescePartitions.enabled': 'true',
               'spark.local.dir': str(out / 'spark-local')}
    for k, v in configs.items():
        cmd += ['--conf', k + '=' + v]
    cmd += [str(HERE / 'experiment.py'), '--phase', a.phase, '--root', str(a.root),
            '--name', a.name, '--profile', a.profile, '--variant', a.variant,
            '--mode', a.mode, '--narrow-option', a.narrow_option]
    files = list((seed / 'bench.db/target').rglob('*')) if a.phase == 'prepare' else []
    metadata = sum(p.stat().st_size for p in files if p.is_file() and p.suffix != '.parquet')
    input_bytes = sum(p.stat().st_size for p in files if p.is_file())
    admission = budget(c, a.phase, metadata, input_bytes, calibrated_bound(a))
    print(json.dumps(dict(command=cmd, budget=admission, seedTargetOnly=str(seed / 'bench.db/target'),
                          metricAvailability=METRIC_AVAILABILITY, executionRequested=a.execute), indent=2), flush=True)
    if not a.execute:
        return

    import psutil  # already used by the old local runner; no installation or SSH here
    require(submit.is_file(), 'spark-submit missing')
    require((a.java_home / 'bin' / ('java.exe' if os.name == 'nt' else 'java')).is_file(), 'Java missing')
    require(all(p.is_file() for p in jars), 'classpath jar missing')
    require(not (a.profile == 'cloud' and a.phase == 'measure') or a.budget_proof is not None,
            'cloud measurement needs a passing current-SDK local 2048-file N pilot calibration; old 26.9 kB/file is not calibration')
    require(not out.exists() if a.phase in ('fixture', 'prepare') else out.is_dir(), 'phase output already exists or is missing')
    require(a.phase == 'fixture' or not out.resolve().is_relative_to(seed), 'output must not be inside seed')
    a.root.mkdir(parents=True, exist_ok=True)
    lock = a.root / 'runner.lock'
    with lock.open('x', encoding='utf-8') as f:
        f.write(str(os.getpid()))
    try:
        active = [p.parent for p in a.root.glob('*/warehouse')
                  if p.parent.name != 'fixture' and not (p.parent / 'retired.json').exists()]
        require(not active or (a.phase in ('measure', 'validate') and active == [out]),
                'archive, verify and retire the preceding cell before launching another')
        require(not any(p.info['name'] and p.info['name'].lower() in ('java', 'java.exe')
                        for p in psutil.process_iter(['name'])), 'another Java process is active')
        # Dirty/writeback quiet gate, used on cloud as in controller-v4.
        if Path('/proc/meminfo').exists():
            consecutive = 0
            deadline = time.monotonic() + 120
            while consecutive < 3:
                require(time.monotonic() < deadline, 'dirty/writeback quiet gate timed out')
                mem = {l.split(':')[0]: int(l.split()[1]) * 1024 for l in Path('/proc/meminfo').read_text().splitlines()}
                consecutive = consecutive + 1 if mem['Dirty'] < 64 * 1024 ** 2 and mem['Writeback'] < 8 * 1024 ** 2 else 0
                time.sleep(2)
        free = shutil.disk_usage(a.root).free
        available = psutil.virtual_memory().available
        require(free >= DISK_FLOOR + admission['additionalDiskBytes'] and
                available >= MEMORY_FLOOR + admission['additionalMemoryBytes'], 'pre-launch capacity admission failed')
        binding = bind(a)
        if a.phase == 'measure':
            require((out / 'source-fairness.json').is_file() and
                    (out / 'prepare-source-io-summary.json').is_file(), 'prepare fairness/source-I/O proof missing')
        if a.phase in ('fixture', 'prepare'):
            out.mkdir()
        require(not (out / (a.phase + '-terminal.json')).exists(), 'phase already completed; no implicit rerun')
        if a.phase == 'prepare':
            original = inventory(seed / 'bench.db/target')
            dump(out / 'seed-target-before.json', dict(path=str(seed / 'bench.db/target'), files=original))
            clone_target(seed, out / 'warehouse')
        dump(out / (a.phase + '-admission.json'), dict(admission, freeBytes=free, availableBytes=available))
        dump(out / (a.phase + '-command.json'), dict(command=cmd, binaryBinding=binding))
        env = os.environ.copy()
        env.update(JAVA_HOME=str(a.java_home.resolve()), SPARK_HOME=str(a.spark.resolve()),
                   SPARK_LOCAL_IP='127.0.0.1', PYSPARK_PYTHON=a.python, PYSPARK_DRIVER_PYTHON=a.python,
                   SPARK_LOCAL_DIRS=str(out / 'spark-local'), SPARK_WORKER_DIR=str(out / 'workers'))
        env['PATH'] = str(a.java_home.resolve() / 'bin') + os.pathsep + env['PATH']
        # Prevent inherited submit flags/Python startup arguments from changing the measured binary.
        for key in ('PYSPARK_SUBMIT_ARGS', 'SPARK_SUBMIT_OPTS', 'JAVA_TOOL_OPTIONS', '_JAVA_OPTIONS'):
            env.pop(key, None)
        violation = False
        min_free, min_available = free, available
        with (out / (a.phase + '.log')).open('xb') as log, (out / (a.phase + '-resources.jsonl')).open('x') as obs:
            process = subprocess.Popen(cmd, stdout=log, stderr=subprocess.STDOUT, env=env)
            try:
                while True:
                    free, available = shutil.disk_usage(a.root).free, psutil.virtual_memory().available
                    min_free, min_available = min(min_free, free), min(min_available, available)
                    samples, procs = process_samples(process.pid, psutil)
                    obs.write(json.dumps(dict(epoch=time.time(), freeBytes=free, availableBytes=available,
                         processTreeRSS=sum(p['rss'] for p in samples), processes=samples)) + '\n')
                    obs.flush()
                    code = process.poll()
                    violation = violation or free < DISK_FLOOR or available < MEMORY_FLOOR
                    if code is not None:
                        break
                    if violation:
                        stop_tree(process, procs, psutil)
                        code = process.returncode
                        break
                    time.sleep(0.5)
            finally:
                if process.poll() is None:
                    _, procs = process_samples(process.pid, psutil)
                    stop_tree(process, procs, psutil)
        dump(out / (a.phase + '-terminal.json'), dict(exitCode=code, reserveViolated=violation,
             minFreeBytes=min_free, minAvailableBytes=min_available))
        require(code == 0 and not violation, 'application failed; retain complete evidence and stop')
        if a.phase != 'fixture':
            source_io_summary(out, a.phase)
        if a.phase in ('prepare', 'validate'):
            before = json.loads((out / 'seed-target-before.json').read_text())
            require(inventory(Path(before['path'])) == before['files'], 'original target changed')
            dump(out / (a.phase + '-original-unchanged.json'), dict(exactInventoryEqual=True))
        if a.phase == 'prepare':
            dataset = json.loads((out / 'source-dataset.json').read_text())
            baselines = a.root / 'source-dataset-baselines'
            baselines.mkdir(exist_ok=True)
            baseline = baselines / (a.variant + '.json')
            if baseline.exists():
                require(json.loads(baseline.read_text()) == dataset, 'source logical/partition/footer layout differs across arms')
            else:
                dump(baseline, dataset)
            dump(out / 'source-fairness.json', dict(exactLogicalAndDatasetFooterEquality=True,
                                                  baselineSHA256=sha(baseline), byteUUIDEqualityRequired=False))
    finally:
        lock.unlink()


if __name__ == '__main__':
    main()
