from pathlib import Path
import sys, os, time, json, subprocess, traceback, shutil, stat, fcntl, pwd, re, math

assert len(sys.argv) == 2, 'usage: run-read-observed.py CONFIG.json'
CONFIG = Path(sys.argv[1]).resolve()
C = json.loads(CONFIG.read_text())
KIT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(KIT / 'harness'))
import bench

def absolute(name):
    p = Path(C[name])
    assert p.is_absolute(), name
    return p.resolve()

B = absolute('workspace'); assert B.is_dir()
R = absolute('output')
assert B in R.parents and R != B and not R.exists(), 'Choose a new output below workspace'
CAN = absolute('canonical')
assert CAN.is_dir() and CAN != R and R not in CAN.parents and CAN not in R.parents
N = C['baseFiles']; assert type(N) is int and N in [64, 32768, 65536]
VARIANT = C['variant']; assert VARIANT in ['upstream', 'off', 'lightweight', 'streaming']
HEAP = C['driverMemory']; assert HEAP in ['512m', '2g']
EXACT_HEAP = C['exactDriverMemory']; assert EXACT_HEAP in ['2g', '4g']
EXACT_EXECUTOR = C['exactExecutorMemory']; assert EXACT_EXECUTOR in ['512m', '1g']
GIB = 1024**3; FLOOR = 5*GIB
PRIVATE = R / 'warehouse'
PERFDATA = Path('/tmp') / ('hsperfdata_' + pwd.getpwuid(os.geteuid()).pw_name)
FROZEN = C['canonicalTreeSha256']
ART = absolute('artifact'); SHA = C['artifactSha256']
HELPER = absolute('readerHelper'); ORACLE = absolute('oracleHelper')
SPARK = absolute('sparkHome'); JAVA = absolute('java17Home')
JSTAT = str(JAVA / 'bin/jstat')
for name in ['canonicalTreeSha256', 'artifactSha256', 'readerHelperSha256', 'oracleHelperSha256']:
    assert len(C[name]) == 64 and all(x in '0123456789abcdef' for x in C[name]), name
proof = json.loads(absolute('provenance').read_text())
assert proof['artifactSha256'] == SHA and proof['buildExitCode'] == 0
assert bench.sha(ART) == SHA and bench.sha(HELPER) == C['readerHelperSha256']
assert bench.sha(ORACLE) == C['oracleHelperSha256']
assert (SPARK / 'bin/spark-submit').is_file() and Path(JSTAT).is_file()
# Only new, synthetic fixture trees are permitted by this controller.
assert C['syntheticScratchInput'] is True
R.mkdir(parents=True, exist_ok=False)

def planned_rss(exact=False):
    if exact:
        return int((int(EXACT_HEAP[:-1]) + (2 if EXACT_EXECUTOR == '1g' else 1) + 2.5)*GIB)
    return int((5.5 if HEAP == '2g' else 5.0)*GIB)

def command(phase, heap=None):
    work = R / phase; work.mkdir()
    events = work / 'events'; events.mkdir()
    local = work / 'spark-local'; local.mkdir()
    exact = phase == 'exact'
    executor = EXACT_EXECUTOR if exact else '512m'
    master = 'local-cluster[2,1,%d]' % (1024 if executor == '1g' else 512)
    gc = '-XX:+UseG1GC -Xlog:gc*:file=' + str(work/'gc-%p.log') + ':time,uptime,level,tags:filecount=3,filesize=16M'
    cmd = [str(SPARK/'bin/spark-submit'), '--master', master,
           '--driver-memory', heap or HEAP, '--executor-memory', executor]
    conf = {'spark.driver.memory': heap or HEAP, 'spark.executor.memory': executor,
            'spark.executor.cores': '1', 'spark.executor.instances': '2',
            'spark.executor.memoryOverhead': '384m', 'spark.sql.shuffle.partitions': '200',
            'spark.dynamicAllocation.enabled': 'false', 'spark.speculation': 'false',
            'spark.task.maxFailures': '1', 'spark.eventLog.enabled': 'true',
            'spark.eventLog.compress': 'false', 'spark.eventLog.dir': events.as_uri(),
            'spark.local.dir': str(local), 'spark.driver.extraJavaOptions': gc,
            'spark.executor.extraJavaOptions': gc}
    for key, value in conf.items(): cmd.extend(['--conf', key+'='+value])
    cmd.extend(['--class', 'bench.CommitMemoryBench' if exact else 'bench.ReadMemoryVersionedAlignedBench',
                '--jars', str(ART)])
    if exact:
        out = work/'exact-evidence'; out.mkdir()
        cmd.extend([str(ORACLE), 'validate', str(PRIVATE), str(out), str(N), '8', '256', '0'])
    else:
        cmd.extend([str(HELPER), str(PRIVATE), phase, str(N), SHA, '256', VARIANT])
    return cmd

def disk_free():
    sv = os.statvfs(B)
    return sv.f_bavail * sv.f_frsize

def effective_available(host, group):
    available = host['MemAvailable']
    if isinstance(group.get('memory.max'), int):
        available = min(available, group['memory.max']-group.get('memory.current', 0))
    return available

def observer_complete(st):
    samples = st['samplers']
    required = {'Timestamp', 'S0U', 'S1U', 'EU', 'OU', 'GCT'}
    return (len(samples)==3 and sum(s['role']=='driver' for s in samples)==1
            and len({(s['pid'],s['startTicks']) for s in samples})==3
            and all(re.search(r'(?:^|\s)-Xmx512[mM](?:\s|$)', s['targetCommand'])
                    for s in samples if s['role']=='executor')
            and all(s['rows']>0 and s['samplerExitCode']==0
                    and s['samplerStartTicks'] is not None
                    and required.issubset(s['columns']) and Path(s['gcLog']).is_file()
                    for s in samples))

def audit_query(text, st, phase, boundary):
    expected_heap = 536870912 if HEAP=='512m' else 2147483648
    assert 'READ_BENCH verifiedJar='+str(ART)+'#'+SHA in text
    assert 'READ_MODE variant='+VARIANT+';' in text
    assert 'READ_BENCH snapshot='+str(boundary)+';schema=1;splitTargetBytes=134217728;openFileCostBytes=4194304;maxDriverHeap='+str(expected_heap) in text
    assert 'READ_BENCH phaseStart=true;' in text and observer_complete(st)
    completions = [l for l in text.splitlines() if l.startswith('READ_BENCH readCompleted=true;')]
    if st['exitCode']==0:
        expected_rows = min(20,2*N+N//5) if phase=='limit20' else 2*N+N//5
        assert len(completions)==1 and re.search(r';rows='+str(expected_rows)+r';readAndValidationNanos=\d+;',completions[0])
        return 'completed-with-consumer-checks'
    assert st['exitCode']==1 and not completions, 'Signal/native/bootstrap/tool failure is not a capacity negative'
    lines = text.splitlines()
    main = [i for i,l in enumerate(lines) if l.startswith('Exception in thread "main" ')]
    assert len(main)==1
    for prefix in ['READ_BENCH verifiedJar='+str(ART)+'#'+SHA, 'READ_MODE variant='+VARIANT+';',
                   'READ_BENCH snapshot=', 'READ_BENCH phaseStart=true;']:
        positions = [i for i,l in enumerate(lines) if l.startswith(prefix)]
        assert len(positions)==1 and positions[0]<main[0], 'Exception must follow the bound identity/config/phase markers'
    chain = [lines[main[0]]]
    for line in lines[main[0]+1:]:
        assert not line.lstrip().startswith('Suppressed:'), 'Ambiguous suppressed exception; inspect raw main chain'
        if (line.startswith('Caused by: ') or re.match(r'^\s+at \S',line)
                or re.match(r'^\s+\.\.\. \d+ more$',line)):
            chain.append(line)
        else:
            break
    causes = [line for line in chain if line.startswith('Caused by: ')]
    root = causes[-1] if causes else chain[0]
    assert re.match(r'^(?:Caused by: |Exception in thread "main" )java\.lang\.OutOfMemoryError: Java heap space(?:$|;)',root), root
    assert st['cgroupOomKillObservedDelta'] in [None,0], 'An observed cgroup kill prevents heap-only classification'
    return 'audited-post-start-java-heap-oom'

def audit_exact(text, st):
    identities = [l for l in text.splitlines() if l.startswith('BENCH driverIdentity=')]
    assert len(identities)==1
    identity = identities[0]
    assert re.search(r'pid='+str(st['pid'])+r';executor=driver;maxHeap='+str(int(EXACT_HEAP[:-1])*1024**3)+r';',identity)
    hashes = re.findall(r'#([a-f0-9]{64})',identity)
    assert len(hashes)==2 and all(x==SHA for x in hashes), identity
    required = {'merged rows', 'unchanged source', 'old key row-ID pairs',
                'complete row-ID domain', 'inserted row-ID domain'}
    checks = [l for l in text.splitlines() if l.startswith('BENCH correctness=')]
    parsed = [re.fullmatch(r'BENCH correctness=([^;]+);unexpected=(\d+);missing=(\d+)',l) for l in checks]
    assert len(parsed)==5 and all(parsed)
    assert {m.group(1) for m in parsed}==required and all(m.group(2)==m.group(3)=='0' for m in parsed)
    assert re.search(r'BENCH validationCompleted=true;expectedRows='+str(2*N+N//5)+r'(?:;|$)',text,re.M)
    return checks, dict(driverIdentity=identity, actualArtifactSha256=hashes[0],
                        observedMaxHeapBytes=int(EXACT_HEAP[:-1])*1024**3, pid=st['pid'],startTicks=st['startTicks'])

def dump(name, data):
    p = R / name
    with p.open('x') as f:
        json.dump(data, f, indent=2); f.write('\n'); f.flush(); os.fsync(f.fileno())

me = bench.proc(os.getpid())
dump('controller-launch.json', dict(utc=time.time(), process=me))

def gate(phase, copy=False, exact=False):
    parts = dict(newClone=sum(p.stat().st_blocks*512 for p in CAN.rglob('*') if p.is_file()) if copy else 0,
                 raw=134217728 if copy else 536870912, runtimeOneApp=2*(ART.stat().st_blocks*512+1048576),
                 controls=134217728, gcLogBound=3*3*16*1024**2,
                 temporaryIO=2147483648, extraOOMGC=1073741824,
                 futureArchives=0, futureQueries=0)
    s = os.statvfs(B); free = s.f_bavail*s.f_frsize
    # The extra half GiB covers three jstat JVMs, each 32 MiB heap plus native overhead.
    rss = planned_rss(exact)
    d = dict(utc=time.time(), phase=phase, parts=parts, freeBytes=free,
             newBytes=sum(parts.values()), floorBytes=FLOOR,
             slackBytes=free-FLOOR-sum(parts.values()), freeInodes=s.f_favail,
             plannedInodes=sum(1 for p in CAN.rglob('*') if p.is_file())+10000 if copy else 10000, inodeReserve=327680,
             memoryGate=bench.reserve(rss), jstatIncludedRssAllowanceBytes=536870912,
             alreadyAllocatedInputNotChargedAgain=not copy, cleanupCredit=0)
    dump(phase+'-budget.json', d)
    assert d['slackBytes'] >= 0 and s.f_favail >= d['plannedInodes']+327680, d
    return d

def run(phase, cmd, observed=False):
    work = R/phase; env = dict(os.environ)
    env['JAVA_HOME'] = str(JAVA)
    env['PATH'] = env['JAVA_HOME']+'/bin:'+env['PATH']
    for k in ['JAVA_TOOL_OPTIONS', '_JAVA_OPTIONS', 'JDK_JAVA_OPTIONS']: env.pop(k, None)
    resource = bench.reserve(planned_rss(phase == 'exact'))
    dump(phase+'-command.json', dict(utc=time.time(), argv=cmd, resource=resource))
    samples = {}; seen = {}; minimum = resource['available']; minfree = disk_free(); peak = 0
    with (work/'stdout.raw.log').open('xb') as log, (work/'resource-observer.jsonl').open('x') as ob:
        p = subprocess.Popen(cmd, cwd=R, env=env, stdout=log, stderr=subprocess.STDOUT, start_new_session=True, pass_fds=(LOCK_FD,))
        identity = bench.proc(p.pid); dump(phase+'-launch.json', dict(utc=time.time(), process=identity))
        while True:
            procs = bench.process_tree(p.pid)
            if observed:
                for q in procs:
                    pid = q['pid']; key = (pid, q['startTicks']); proc = Path('/proc')/str(pid)
                    try:
                        c = (proc/'cmdline').read_bytes().replace(b'\0', b' ').decode(errors='replace')
                        if (proc/'comm').read_text().strip() != 'java': continue
                        if pid != p.pid and 'CoarseGrainedExecutorBackend' not in c: continue
                        if key in samples or not (PERFDATA/str(pid)).exists(): continue
                        current = bench.proc(pid)
                        if not current or current['startTicks'] != key[1]: continue
                        sp = work/('jstat-%d-%d.log' % key); f = sp.open('xb')
                        argv = [JSTAT, '-J-Xmx32m', '-gc', '-t', str(pid), '1000', '7200']
                        sampler = subprocess.Popen(argv, stdin=subprocess.DEVNULL, stdout=f, stderr=subprocess.STDOUT, env=env, start_new_session=True, pass_fds=(LOCK_FD,))
                        sid = bench.proc(sampler.pid)
                        samples[key] = dict(process=sampler, samplerStartTicks=sid['startTicks'] if sid else None, stream=f, log=sp, argv=argv,
                                            role='driver' if pid == p.pid else 'executor', startUtc=time.time(), command=c)
                    except FileNotFoundError: pass
            extra = [q for q in (bench.proc(x['process'].pid) for x in samples.values()) if q]
            memory = bench.memory(); group = bench.cgroup(os.getpid()); free = disk_free()
            effective = effective_available(memory,group)
            minimum = min(minimum, effective); minfree = min(minfree, free)
            peak = max(peak, sum(q['rssBytes'] for q in procs+extra)); seen.update({q['pid']:q['startTicks'] for q in procs})
            ob.write(json.dumps(dict(utc=time.time(), productProcesses=procs, samplerProcesses=extra, memory=memory, cgroup=group, effectiveAvailableBytes=effective, diskFreeBytes=free))+'\n'); ob.flush()
            code = p.poll()
            if code is not None: break
            time.sleep(0.5)
    sampler_records = []
    for key, x in samples.items():
        try: scode = x['process'].wait(timeout=15)
        except subprocess.TimeoutExpired: scode = None
        x['stream'].close()
        lines = x['log'].read_text(errors='replace').splitlines(); headers = next((l.split() for l in lines if l.strip().startswith('Timestamp')), [])
        rows = []
        for l in lines:
            fields = l.split()
            if headers and len(fields) == len(headers):
                try: rows.append(dict(zip(headers, map(float, fields))))
                except ValueError: pass
        if rows:
            assert {'Timestamp','S0U','S1U','EU','OU','GCT'}.issubset(headers)
            assert all(math.isfinite(v[k]) for v in rows for k in ['Timestamp','S0U','S1U','EU','OU','GCT'])
        sampler_records.append(dict(pid=key[0], startTicks=key[1], role=x['role'], samplerPid=x['process'].pid, samplerStartTicks=x['samplerStartTicks'],
            samplerExitCode=scode, argv=x['argv'], startUtc=x['startUtc'], endUtc=time.time(), log=str(x['log']),
            rows=len(rows), columns=headers, first=rows[0] if rows else None, last=rows[-1] if rows else None,
            interpretation='capacity/use columns KiB; Timestamp and GC-time columns seconds; GC counts are counts; observed JVM lifetime, not query-only or exact pause time',
            maxObservedUsedHeapBytes=max(((v['S0U']+v['S1U']+v['EU']+v['OU'])*1024 for v in rows), default=None),
            gcLog=str(work/('gc-%d.log' % key[0])), targetCommand=x['command']))
    lingering = [q for q in (bench.proc(pid) for pid in seen) if q and q['state'] != 'Z' and q['startTicks'] == seen[q['pid']]]
    lingering_samplers = [q for x in samples.values() for q in [bench.proc(x['process'].pid)]
                          if q and q['state']!='Z' and q['startTicks']==x['samplerStartTicks']]
    cg_start = resource['cgroup'].get('memory.events',{}).get('oom_kill')
    cg_end = bench.cgroup(os.getpid()).get('memory.events',{}).get('oom_kill')
    st = dict(utc=time.time(), pid=p.pid, startTicks=identity['startTicks'], exitCode=code,
              minAvailableBytes=minimum, minDiskFreeBytes=minfree, peakRssIncludingSamplersBytes=peak,
              lingering=lingering, memoryReserveViolated=minimum<resource['requiredReserveBytes'], diskFloorViolated=minfree<FLOOR,
              samplers=sampler_records, observed=observed, lingeringSamplers=lingering_samplers,
              cgroupOomKillObservedDelta=cg_end-cg_start if cg_start is not None and cg_end is not None else None)
    dump(phase+'-terminal.json', st)
    assert not lingering and not lingering_samplers and not st['memoryReserveViolated'] and not st['diskFloorViolated'], st
    assert st['cgroupOomKillObservedDelta'] in [None,0], 'Observed cgroup kill stops every phase'
    # Preserve both registered capacity outcomes, including an expected negative control.
    if observed and code == 0:
        assert len(sampler_records) == 3 and sum(x['role']=='driver' for x in sampler_records)==1
        assert all(x['rows']>0 and x['samplerExitCode'] == 0 and Path(x['gcLog']).exists() for x in sampler_records), sampler_records
    return st

try:
    with (B/'retained-v3-coordinator.lock').open('a+b') as coordinator, (B/'reproduction-runner.lock').open('a+b') as lock:
        fcntl.flock(coordinator, fcntl.LOCK_EX|fcntl.LOCK_NB)
        fcntl.flock(lock, fcntl.LOCK_EX|fcntl.LOCK_NB)
        LOCK_FD = lock.fileno()
        assert subprocess.run(['pgrep','-x','java|jstat'],capture_output=True).returncode==1
        assert bench.sha(ART)==SHA and bench.sha(HELPER)==C['readerHelperSha256']
        assert bench.sha(ORACLE)==C['oracleHelperSha256']
        dump('registration.json', dict(utc=time.time(), sourceSha256=bench.sha(Path(__file__)), artifactSha256=SHA, procClockTicks=os.sysconf('SC_CLK_TCK'),
             helperSha256=bench.sha(HELPER), oracleSha256=bench.sha(ORACLE), canonical=str(CAN), canonicalTree=FROZEN,
             baseFiles=N, extraColumns=256, updatedColumns=128, expectedRows=2*N+N//5,
             physicalClone=True, mode=VARIANT, prepareOnce=True, driver=HEAP, executors=['512m','512m'],
             shuffle=200, splitTarget=134217728, openCost=4194304, observations='1s jstat -gc -t plus bounded GC logs; no forced GC, JFR, HPROF',
             readOrder=['limit20','full'], separateActualReaderExactDriver=EXACT_HEAP, exactExecutor=EXACT_EXECUTOR, configuration=C, configurationSha256=bench.sha(CONFIG),
             noPerformanceRatio=True, floor=FLOOR, perArchiveCAP=844070476))
        gate('clone-prepare', copy=True)
        assert bench.inventory(CAN, R/'canonical-before.jsonl')==FROZEN
        boundary=int((CAN/'bench.db/target/snapshot/LATEST').read_text())
        shutil.copytree(CAN, PRIVATE)
        assert bench.inventory(PRIVATE,R/'copy-before.jsonl')==FROZEN
        count=0
        for p in PRIVATE.rglob('*'):
            s=p.lstat(); assert not stat.S_ISLNK(s.st_mode)
            if stat.S_ISREG(s.st_mode):
                src=(CAN/p.relative_to(PRIVATE)).stat(); assert s.st_nlink==1 and (s.st_dev,s.st_ino)!=(src.st_dev,src.st_ino); count+=1
        dump('physical-copy-proof.json',dict(utc=time.time(),files=count,sharedInodes=0,canonicalTree=FROZEN))
        assert run('prepare',command('prepare'))['exitCode']==0, 'Preparation failed; queries must not proceed'
        s0=json.loads((PRIVATE/'bench.db/target/schema/schema-0').read_text());s1=json.loads((PRIVATE/'bench.db/target/schema/schema-1').read_text())
        changes={k:[s0['options'].get(k),s1['options'].get(k)] for k in set(s0['options'])|set(s1['options']) if s0['options'].get(k)!=s1['options'].get(k)}
        expected={'snapshot.num-retained.min':[None,'10']}
        if VARIANT!='upstream': expected['data-evolution.metadata-memory-optimization.mode']=[None,VARIANT]
        assert s0['fields']==s1['fields'] and changes==expected
        assert int((PRIVATE/'bench.db/target/snapshot/LATEST').read_text())==boundary
        prepared=bench.inventory(PRIVATE,R/'prepared-before-queries.jsonl')
        (R/'captured-schema-1.json').write_bytes((PRIVATE/'bench.db/target/schema/schema-1').read_bytes())
        dump('prepared.json',dict(treeSha256=prepared,snapshotBefore=boundary,snapshotAfter=boundary,fieldsIdentical=True,changes=changes))
        outcomes={}
        for phase in ['limit20','full']:
            gate(phase); st=run(phase,command(phase),observed=True)
            text=(R/phase/'stdout.raw.log').read_text(errors='replace')
            assert bench.inventory(PRIVATE,R/(phase+'-after.jsonl'))==prepared and bench.sha(ART)==SHA
            classification = audit_query(text,st,phase,boundary)
            outcomes[phase]=dict(terminal=st,classification=classification,markers=[l for l in text.splitlines() if l.startswith(('READ_BENCH','READ_MODE'))])
            dump(phase+'-result.json',outcomes[phase])
        gate('exact',exact=True); st=run('exact',command('exact',EXACT_HEAP))
        assert st['exitCode']==0, 'Independent exact validation failed'
        text=(R/'exact/stdout.raw.log').read_text(errors='replace'); checks,identity=audit_exact(text,st)
        assert bench.inventory(PRIVATE,R/'exact-after.jsonl')==prepared
        exact=dict(terminal=st,checks=checks,allFivePassed=True,actualReaderArtifactSha256=identity['actualArtifactSha256'], runtimeIdentity=identity)
        dump('exact-result.json',exact)
        assert bench.inventory(CAN,R/'canonical-after.jsonl')==FROZEN and bench.sha(ART)==SHA
        dump('complete.json',dict(utc=time.time(),passed=all(v['terminal']['exitCode']==0 for v in outcomes.values()),baseFiles=N,artifactSha256=SHA,readResults=outcomes,
            observerCoveragePassed=all(v['terminal']['exitCode']==0 for v in outcomes.values()),independentFiveExact=exact,canonicalUnchanged=True,preparedUnchanged=True,
            preparedTreeSha256=prepared,noPerformanceRatioClaim=True,archivePending=True))
        code=0 if all(v['terminal']['exitCode']==0 for v in outcomes.values()) else 2
except BaseException:
    traceback.print_exc();dump('controller-failure.json',dict(utc=time.time(),error=traceback.format_exc()));code=1
dump('controller-terminal.json',dict(utc=time.time(),pid=os.getpid(),startTicks=me['startTicks'],exitCode=code))
sys.exit(code)
