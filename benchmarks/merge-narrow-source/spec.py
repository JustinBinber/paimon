"""Workload contract shared by the Spark experiment and launch planner (stdlib only)."""
import hashlib
import json
import os
from pathlib import Path

GIB = 1024 ** 3
DISK_FLOOR = 5 * GIB
MEMORY_FLOOR = 3435973837  # ceil(3.2 GiB)
ORIGINAL_CLOUD_SEED = os.environ.get('PAIMON_BENCH_SEED', 'SET_SEED_WITH_CLI')
ORDERS = ('FRN', 'RNF', 'NFR')
VARIANTS = ('insert17', 'insert50', 'insert90', 'insert100', 'insert90-idpayload')
NAMES = ['id', 'value', 'payload'] + ['x%d' % i for i in range(256)]
UPDATE_NAMES = ['value'] + ['x%d' % i for i in range(127)]
MODES = {'F': (False, False), 'R': (True, False), 'N': (True, True)}
NARROW_OPTION = 'data-evolution.merge-into.join-reuse.narrow-source.enabled'
SOURCE_PARTITIONS = 2
METRIC_AVAILABILITY = dict(realSourceFileIO='bench-counted source-only stream counters: final-SDK Java compile and direct service/stream smoke passed; Spark attribution must pass prepare gate',
                         taskInputBytes='all task inputs, not source file I/O',
                         processReadBytes='whole-process OS physical I/O; warm-cache reads may be zero')


def require(ok, message):
    if not ok:
        raise ValueError(message)


def case(profile, variant):
    require(profile in ('local', 'cloud') and variant in VARIANTS, 'unknown case')
    files, source = (2048, 2457) if profile == 'local' else (32768, 39321)
    matched = dict(insert17=files, insert50=source // 2,
                   insert90=round(source / 10), insert100=0,
                   **{'insert90-idpayload': round(source / 10)})[variant]
    return dict(profile=profile, variant=variant, targetFiles=files, rowsPerFile=2,
                oldRows=files * 2, sourceRows=source, matched=matched,
                inserted=source - matched, columns=259, updateColumns=128,
                exclusivePayload=variant != 'insert90-idpayload')


def schedule(profile):
    return [dict(case(profile, v), round=r + 1, mode=m,
                 name='%s-r%d-%s' % (v, r + 1, m))
            for v in VARIANTS for r, order in enumerate(ORDERS) for m in order]


def payload(ident, exclusive):
    if exclusive:
        return ("concat_ws('',transform(sequence(0,127),i->sha2(concat(cast(%s as string),"
                "':',cast(i as string)),256)))" % ident)
    return "CASE WHEN pmod(%s,7)=0 THEN CAST(NULL AS STRING) ELSE concat('payload-',%s) END" % (ident, ident)


def original_sql(c):
    fields = ['id', 'id*7L', payload('id', False)] + [
        'CASE WHEN pmod(id+%d,11)=0 THEN CAST(NULL AS BIGINT) ELSE id*%dL END' % (i, i + 3)
        for i in range(256)]
    return 'SELECT ' + ','.join(x + ' AS ' + n for x, n in zip(fields, NAMES)) + ' FROM range(%d)' % c['oldRows']


def source_sql(c):
    # One even ID per touched two-row file, followed by a disjoint new-ID range.
    ids = ('SELECT id*2L AS id FROM range(%d) UNION ALL SELECT id FROM range(%d,%d)'
           % (c['matched'], c['oldRows'], c['oldRows'] + c['inserted']))
    fields = ['id', ('CASE WHEN id<%d AND pmod(id,10)=0 THEN CAST(NULL AS BIGINT) '
                     'ELSE -id-100L END AS value') % c['oldRows']]
    if c['exclusivePayload']:
        fields.append(payload('id', True) + ' AS insert_payload')
    return 'SELECT ' + ','.join(fields) + ' FROM (' + ids + ') ids'


def merge_sql(c):
    updates = ['t.value=s.value'] + [
        't.x%d=CASE WHEN pmod(s.id+%d,11)=0 THEN CAST(NULL AS BIGINT) ELSE s.id*%dL+1000L END'
        % (i, i, i + 3) for i in range(127)]
    fields = ['s.id', 's.value', 's.insert_payload' if c['exclusivePayload'] else payload('s.id', False)]
    fields += ['CASE WHEN pmod(s.id+%d,11)=0 THEN CAST(NULL AS BIGINT) ELSE s.id*%dL END'
               % (i, i + 3) for i in range(256)]
    return ('MERGE INTO paimon.bench.target t USING paimon.bench.source s ON t.id=s.id '
            'WHEN MATCHED THEN UPDATE SET ' + ','.join(updates) +
            ' WHEN NOT MATCHED THEN INSERT (' + ','.join(NAMES) + ') VALUES (' + ','.join(fields) + ')')


def expected_sql(c):
    # Independently reconstruct every old row and insert, without reading the source.
    hit = 'pmod(id,2)=0 AND id<%d' % (2 * c['matched'])
    fields = ['id', ('CASE WHEN %s THEN CASE WHEN pmod(id,10)=0 THEN CAST(NULL AS BIGINT) '
                     'ELSE -id-100L END ELSE id*7L END') % hit, payload('id', False)]
    for i in range(256):
        delta = ' + CASE WHEN %s THEN 1000L ELSE 0L END' % hit if i < 127 else ''
        fields.append('CASE WHEN pmod(id+%d,11)=0 THEN CAST(NULL AS BIGINT) ELSE id*%dL%s END'
                      % (i, i + 3, delta))
    old = 'SELECT ' + ','.join(x + ' AS ' + n for x, n in zip(fields, NAMES)) + ' FROM range(%d)' % c['oldRows']
    fields = ['id', '-id-100L', payload('id', c['exclusivePayload'])] + [
        'CASE WHEN pmod(id+%d,11)=0 THEN CAST(NULL AS BIGINT) ELSE id*%dL END' % (i, i + 3)
        for i in range(256)]
    return old + ' UNION ALL SELECT ' + ','.join(fields) + ' FROM range(%d,%d)' % (c['oldRows'], c['oldRows'] + c['inserted'])


def budget(c, phase, metadata_bytes=0, input_bytes=0, partial_file_upper=None):
    # Explicit conservative planning estimates, never a fixed historical output constant.
    source_row = 16 + (8192 if c['exclusivePayload'] else 0)
    insert_row = 259 * 8 + (8192 if c['exclusivePayload'] else 64)
    # Do not lower the default using old-SDK evidence. Main supplies a new-SDK pilot bound.
    per_file = partial_file_upper or (2 * 128 * 8 + 128 * 384 + 4096)
    partial = c['matched'] * per_file
    inserted = c['inserted'] * insert_row
    source = c['sourceRows'] * source_row
    scratch = max(128 * 1024 ** 2, 2 * (source + inserted))
    output = partial + inserted if phase == 'measure' else source if phase == 'prepare' else 0
    if phase == 'fixture':
        output = c['targetFiles'] * (2 * 259 * 8 + 259 * 384 + 4096)
    return dict(phase=phase, targetOutputEstimate=partial + inserted, sourceEstimate=source,
                partialFileUpperBytes=per_file, partialBudgetCalibrated=partial_file_upper is not None,
                inputBytes=input_bytes, metadataCopyBytes=metadata_bytes,
                additionalDiskBytes=metadata_bytes + output + scratch + 128 * 1024 ** 2,
                additionalMemoryBytes=(2 if c['profile'] == 'local' else 8) * GIB,
                diskFloorBytes=DISK_FLOOR, availableMemoryFloorBytes=MEMORY_FLOOR,
                estimate='per variant rows, width, touched-file footer allowance, scratch; monitor remains mandatory')


def sha(path):
    h = hashlib.sha256()
    with Path(path).open('rb') as f:
        for b in iter(lambda: f.read(1024 * 1024), b''):
            h.update(b)
    return h.hexdigest()


def inventory(root):
    root = Path(root)
    require(root.is_dir() and not root.is_symlink(), 'missing or linked inventory root')
    result = {}
    for p in sorted(root.rglob('*')):
        require(not p.is_symlink(), 'symlink in evidence tree')
        if p.is_file():
            result[p.relative_to(root).as_posix()] = dict(bytes=p.stat().st_size, sha256=sha(p))
    return result


def dump(path, value):
    path = Path(path)
    temp = path.with_name(path.name + '.tmp')
    with temp.open('x', encoding='utf-8') as f:
        json.dump(value, f, indent=2)
        f.flush()
        os.fsync(f.fileno())
    os.replace(str(temp), str(path))
    if os.name != 'nt':
        fd = os.open(str(path.parent), os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)


def self_check():
    for profile, pairs in [('cloud', [(32768, 6553), (19660, 19661), (3932, 35389), (0, 39321), (3932, 35389)]),
                           ('local', [(2048, 409), (1228, 1229), (246, 2211), (0, 2457), (246, 2211)])]:
        for v, counts in zip(VARIANTS, pairs):
            c = case(profile, v)
            require((c['matched'], c['inserted']) == counts, 'count regression')
            require(c['matched'] + c['inserted'] == c['sourceRows'], 'source cardinality')
            require('insert_payload' not in merge_sql(c) if not c['exclusivePayload'] else True, 'payload leaked')
        rows = schedule(profile)
        require(len(rows) == 45, 'matrix size')
        for v in VARIANTS:
            require([''.join(x['mode'] for x in rows if x['variant'] == v and x['round'] == r)
                     for r in (1, 2, 3)] == list(ORDERS), 'unbalanced rounds')


if __name__ == '__main__':
    self_check()
    print(json.dumps({p: schedule(p) for p in ('local', 'cloud')}, indent=2))
