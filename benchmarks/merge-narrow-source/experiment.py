"""One Spark application per phase. No SSH, compilation, or coordinator side effects."""
import argparse
import json
import time
from pathlib import Path

from spec import (METRIC_AVAILABILITY, MODES, NAMES, NARROW_OPTION, SOURCE_PARTITIONS,
                  UPDATE_NAMES, VARIANTS, case, dump, expected_sql,
                  inventory, merge_sql, original_sql, require, source_sql)


def arguments():
    p = argparse.ArgumentParser(__doc__)
    p.add_argument('--phase', required=True, choices=('fixture', 'prepare', 'measure', 'validate'))
    p.add_argument('--root', required=True, type=Path)
    p.add_argument('--name', required=True)
    p.add_argument('--profile', choices=('local', 'cloud'), required=True)
    p.add_argument('--variant', choices=VARIANTS, required=True)
    p.add_argument('--mode', choices=tuple(MODES), required=True)
    p.add_argument('--narrow-option', default=NARROW_OPTION, help='Confirmed production table option')
    return p.parse_args()


def snapshot(warehouse):
    return max(int(p.name.split('-')[1]) for p in
               (warehouse / 'bench.db/target/snapshot').glob('snapshot-*'))


def main():
    a = arguments()
    require(a.name and Path(a.name).name == a.name and a.name not in ('.', '..'), 'unsafe cell name')
    require(a.narrow_option.startswith('data-evolution.') and
            all(ch.isalnum() or ch in '.-' for ch in a.narrow_option) and
            a.narrow_option != 'data-evolution.merge-into.join-reuse', 'invalid production option')
    c = case(a.profile, a.variant)
    out = a.root.resolve() / a.name
    require(out.is_dir(), 'launcher must create the private cell directory')
    warehouse = out / 'warehouse'
    from pyspark import SparkContext
    from pyspark.sql import SparkSession
    SparkContext._ensure_initialized()
    SparkContext._jvm.java.lang.System.setProperty('bench.events', str(out / (a.phase + '-metrics.jsonl')))
    spark = (SparkSession.builder.appName('merge-production-' + a.name + '-' + a.phase)
             .config('spark.sql.extensions', 'org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions')
             .config('spark.sql.catalog.paimon', 'org.apache.paimon.spark.SparkCatalog')
             .config('spark.sql.catalog.paimon.metastore', 'bench-counted')
             .config('spark.sql.catalog.paimon.bench.metrics-directory', str(out / (a.phase + '-source-io')))
             .config('spark.sql.catalog.paimon.warehouse', warehouse.as_uri())
             .config('spark.extraListeners', 'bench.NarrowMetrics')
             .config('spark.eventLog.enabled', 'false')
             .config('spark.eventLog.logBlockUpdates.enabled', 'true')
             .config('spark.sql.debug.maxToStringFields', '400').getOrCreate())
    spark.sparkContext.setLogLevel('ERROR')
    checks = {}

    def same(actual, expected, label):
        unexpected = actual.exceptAll(expected).count()
        missing = expected.exceptAll(actual).count()
        require(unexpected == missing == 0, '%s: unexpected=%d missing=%d' % (label, unexpected, missing))
        checks[label] = True

    def inspect(filename):
        spark._jvm.bench.NarrowInspect.inspect(str(warehouse), str(out / filename))
        return json.loads((out / filename).read_text())

    try:
        if a.phase == 'fixture':
            require(a.profile == 'local', 'cloud uses the frozen original target; never generate a replacement')
            args = spark.sparkContext._gateway.new_array(spark._jvm.java.lang.String, 7)
            evidence = out / 'fixture-evidence'
            evidence.mkdir()
            for i, v in enumerate(['fixture', str(warehouse), str(evidence), '2048', '2', '256', '0']):
                args[i] = v
            spark._jvm.bench.CommitMemoryBench.main(args)
            dump(out / 'fixture-result.json', dict(case=c, snapshot=snapshot(warehouse)))
            return

        target = 'paimon.bench.target'
        source = 'paimon.bench.source'
        if a.phase == 'prepare':
            # The launcher clones only the original target, never an arbitrary-ratio prepared source.
            spark.sql('CREATE DATABASE IF NOT EXISTS paimon.bench')
            require(not (warehouse / 'bench.db/source').exists(), 'source must be freshly generated')
            require(not (warehouse / 'bench.db/old_pairs').exists(), 'old_pairs must be fresh')
            properties = dict((r[0], r[1]) for r in spark.sql('SHOW TBLPROPERTIES ' + target).collect())
            for key, value in [('bucket', '-1'), ('data-evolution.enabled', 'true'),
                               ('row-tracking.enabled', 'true'), ('file.format', 'parquet')]:
                require(properties.get(key) == value, 'target property mismatch: ' + key)
            require(spark.table(target).columns == NAMES, 'target must have exactly 259 business columns')
            same(spark.sql('SELECT ' + ','.join(NAMES) + ' FROM ' + target), spark.sql(original_sql(c)), 'originalFullMultiset')
            same(spark.sql('SELECT id,_ROW_ID AS row_id FROM ' + target),
                 spark.sql('SELECT id,id AS row_id FROM range(%d)' % c['oldRows']), 'originalRowIDMapping')
            initial = inspect('effective-before.json')
            require(len(initial['files']) == c['targetFiles'] and
                    all(f['rows'] == 2 for f in initial['files']), 'target must be fixed N x 2 files')
            spark.sql("CREATE TABLE paimon.bench.old_pairs TBLPROPERTIES ('bucket'='-1') AS "
                      'SELECT id,_ROW_ID AS row_id FROM ' + target)
            generation = ('SELECT /*+ REPARTITION(%d, id) */ * FROM (%s) regenerated SORT BY id'
                          % (SOURCE_PARTITIONS, source_sql(c)))
            (out / 'source.sql').write_text(generation, encoding='utf-8')
            adaptive = spark.conf.get('spark.sql.adaptive.enabled')
            spark.conf.set('spark.sql.adaptive.enabled', 'false')
            try:
                spark.sql("CREATE TABLE " + source + " TBLPROPERTIES ('bucket'='-1','file.format'='parquet') AS " + generation)
            finally:
                spark.conf.set('spark.sql.adaptive.enabled', adaptive)
            same(spark.table(source), spark.sql(source_sql(c)), 'freshSourceFullMultiset')
            shape = spark.sql('SELECT count(*),count(DISTINCT id),sum(CASE WHEN id<%d THEN 1 ELSE 0 END) FROM %s'
                              % (c['oldRows'], source)).collect()[0]
            require(tuple(shape) == (c['sourceRows'], c['sourceRows'], c['matched']), 'source cardinality mismatch')
            source_cols = ['id', 'value'] + (['insert_payload'] if c['exclusivePayload'] else [])
            logical = spark.sql('SELECT count(*) AS rows,min(id) AS minId,max(id) AS maxId,'
                                'sum(CASE WHEN value IS NULL THEN 1 ELSE 0 END) AS nullValues,'
                                'cast(sum(cast(xxhash64(%s) as decimal(38,0))) as string) AS hashSum FROM %s'
                                % (','.join(source_cols), source)).collect()[0].asDict()
            spark._jvm.bench.NarrowInspect.inspectSource(str(warehouse), str(out / 'source-footers.json'))
            footers = json.loads((out / 'source-footers.json').read_text())
            # Names/UUIDs and whole-file byte hashes are evidence, not a fairness equality condition.
            canonical = sorted([f['datasetFooter'] for f in footers], key=lambda f: json.dumps(f, sort_keys=True))
            dump(out / 'source-dataset.json', dict(logical=logical, fullMultisetAgainstGenerator=True,
                 partitionContract=dict(partitions=SOURCE_PARTITIONS, expression='hash(id)',
                                        withinPartitionSort='id ascending', adaptiveDuringGeneration=False),
                 canonicalFooters=canonical))
            reuse, narrow = MODES[a.mode]
            spark.sql("ALTER TABLE " + target + " SET TBLPROPERTIES ('data-evolution.merge-into.join-reuse'='%s','%s'='%s')"
                      % (str(reuse).lower(), a.narrow_option, str(narrow).lower()))
            effective = dict((r[0], r[1]) for r in spark.sql('SHOW TBLPROPERTIES ' + target).collect())
            require(effective.get(a.narrow_option) == str(narrow).lower(), 'table option not persisted')
            require(effective.get('data-evolution.merge-into.join-reuse') == str(reuse).lower(), 'reuse option mismatch')
            dump(out / 'source-before.json', inventory(warehouse / 'bench.db/source'))
            dump(out / 'old-pairs-before.json', inventory(warehouse / 'bench.db/old_pairs'))
            dump(out / 'prepared.json', dict(case=c, mode=a.mode, narrowOption=a.narrow_option,
                                            tableProperties=effective, checks=checks,
                                            snapshot=snapshot(warehouse), regeneratedBeforeTimer=True))
            return

        prepared = json.loads((out / 'prepared.json').read_text())
        require(prepared['case'] == c and prepared['mode'] == a.mode and
                prepared['narrowOption'] == a.narrow_option, 'cell contract changed')
        cat = spark._jsparkSession.sessionState().catalogManager().catalog('paimon')
        ns = spark.sparkContext._gateway.new_array(spark._jvm.java.lang.String, 1)
        ns[0] = 'bench'
        locations = {}
        for name in ('target', 'source', 'old_pairs'):
            ident = spark._jvm.org.apache.spark.sql.connector.catalog.Identifier.of(ns, name)
            loc = cat.loadTable(ident).getTable().location().toUri().normalize().getPath()
            expected = (warehouse / 'bench.db' / name).as_posix()
            require(loc.replace('\\', '/').lstrip('/').rstrip('/').casefold() ==
                    expected.lstrip('/').rstrip('/').casefold(), 'table escaped private warehouse')
            locations[name] = loc
        dump(out / (a.phase + '-locations.json'), locations)
        require(inventory(warehouse / 'bench.db/source') == json.loads((out / 'source-before.json').read_text()), 'source changed')
        if a.phase == 'measure':
            require(not (out / 'sql-result.json').exists(), 'refuse to rerun a measured cell')
            require(snapshot(warehouse) == prepared['snapshot'], 'target snapshot changed before timer')
            # Warm OS page cache using complete inputs. No Spark cache or stable-source assertion.
            warmed = 0
            start = time.perf_counter()
            for table in ('target', 'source'):
                for p in sorted((warehouse / 'bench.db' / table).rglob('*')):
                    if p.is_file():
                        with p.open('rb') as f:
                            for block in iter(lambda: f.read(1024 * 1024), b''):
                                warmed += len(block)
            dump(out / 'warmcache.json', dict(method='complete sequential OS reads; fresh JVM; no Spark persist',
                                            bytes=warmed, seconds=time.perf_counter() - start))
            sql = merge_sql(c)
            (out / 'sql.txt').write_text(sql, encoding='utf-8')
            before = snapshot(warehouse)
            spark.sparkContext.setJobGroup('measured-merge', 'complete MERGE SQL including command execution')
            begin = time.time()
            start = time.perf_counter()
            spark.sql(sql).collect()
            elapsed = time.perf_counter() - start
            end = time.time()
            for key in ('spark.jobGroup.id', 'spark.job.description', 'spark.job.interruptOnCancel'):
                spark.sparkContext.setLocalProperty(key, None)
            after = snapshot(warehouse)
            require(after == before + 1, 'MERGE must commit exactly one snapshot')
            dump(out / 'sql-result.json', dict(case=c, mode=a.mode, sqlSeconds=elapsed,
                 sqlStartEpoch=begin, sqlEndEpoch=end, snapshotBefore=before, snapshotAfter=after,
                 spark=spark.version, applicationId=spark.sparkContext.applicationId,
                 fullSQL=True, cachePolicy='warm OS cache, no Spark persist',
                 allInsertMayUseFallback=c['matched'] == 0))
            return

        result = json.loads((out / 'sql-result.json').read_text())
        require(snapshot(warehouse) == result['snapshotAfter'] == result['snapshotBefore'] + 1, 'snapshot changed')
        checks['singleSnapshot'] = True
        same(spark.sql('SELECT ' + ','.join(NAMES) + ' FROM ' + target), spark.sql(expected_sql(c)), 'fullMultiset')
        same(spark.sql('SELECT id,_ROW_ID AS row_id FROM %s WHERE id<%d' % (target, c['oldRows'])),
             spark.table('paimon.bench.old_pairs'), 'oldRowIDs')
        final_rows = c['oldRows'] + c['inserted']
        same(spark.sql('SELECT _ROW_ID AS row_id FROM ' + target),
             spark.sql('SELECT id AS row_id FROM range(%d)' % final_rows), 'allRowIDDomain')
        same(spark.sql('SELECT _ROW_ID AS row_id FROM %s WHERE id>=%d' % (target, c['oldRows'])),
             spark.sql('SELECT id AS row_id FROM range(%d,%d)' % (c['oldRows'], final_rows)), 'newRowIDDomain')
        same(spark.table(source), spark.sql(source_sql(c)), 'sourceFullMultiset')
        require(inventory(warehouse / 'bench.db/source') == json.loads((out / 'source-before.json').read_text()), 'source inventory changed')
        require(inventory(warehouse / 'bench.db/old_pairs') == json.loads((out / 'old-pairs-before.json').read_text()), 'old_pairs inventory changed')
        checks['sourceInventory'] = checks['oldPairsInventory'] = True
        before = json.loads((out / 'effective-before.json').read_text())
        after = inspect('effective-after.json')
        old = {(f['bucket'], f['name']) for f in before['files']}
        new = [f for f in after['files'] if (f['bucket'], f['name']) not in old]
        require(all(f['actualBytes'] == f['bytes'] for f in after['files']), 'manifest/file byte size differs')
        partial = [f for f in new if f['firstRowId'] < c['oldRows']]
        inserted = [f for f in new if f['firstRowId'] >= c['oldRows']]
        require(len(partial) == c['matched'], 'unexpected number of partial column files')
        require(sorted(f['firstRowId'] for f in partial) == list(range(0, 2 * c['matched'], 2)), 'partial ranges mismatch')
        require(all(f['rows'] == 2 and set(f['writeCols'] or []) == set(UPDATE_NAMES)
                    for f in partial), 'partial files must contain UPDATE128 columns and two rows')
        next_id = c['oldRows']
        for f in sorted(inserted, key=lambda f: f['firstRowId']):
            require(f['firstRowId'] == next_id and f['rows'] > 0 and
                    (f['writeCols'] is None or set(f['writeCols']) == set(NAMES)), 'insert file range/columns mismatch')
            next_id += f['rows']
        require(next_id == final_rows and after['snapshot'] == result['snapshotAfter'], 'insert domain/layout mismatch')
        require(old.issubset({(f['bucket'], f['name']) for f in after['files']}), 'untouched original column files were lost')
        checks['fileLayout'] = True
        dump(out / 'file-layout.json', dict(partialFiles=len(partial), partialRows=sum(f['rows'] for f in partial),
             partialBytes=sum(f['bytes'] for f in partial), insertFiles=len(inserted), insertRows=c['inserted'],
             insertBytes=sum(f['bytes'] for f in inserted), newFiles=new, dataEvolution=True))
        result['checks'] = checks
        # Compare the INSERT range whose file count can change with join reuse.
        # The complete oracle above warms inputs equally; never collect wide rows to the driver.
        select_sql = ('SELECT count(*),sum(length(payload)) FROM %s WHERE id>=%d'
                      % (target, c['oldRows']))
        select_samples = []
        for iteration in range(2):
            begin = time.perf_counter()
            values = tuple(spark.sql(select_sql).collect()[0])
            seconds = time.perf_counter() - begin
            require(values[0] == c['inserted'], 'INSERT-range read probe row count differs')
            select_samples.append(dict(seconds=seconds, rows=values[0], payloadCharacters=values[1]))
        result['selectProbe'] = dict(sql=select_sql, samples=select_samples,
            cachePolicy='warm OS/metadata after identical complete oracle; no Spark persist',
            scope='INSERT-range id/payload read, not a general SELECT benchmark')
        result['metricCaveats'] = 'taskInputBytes mixes target/source/metadata and retries; it is not source bytes. Inspect plan/cache events; all-INSERT may have no shared branch.'
        result['metricAvailability'] = METRIC_AVAILABILITY
        result['sourceFileIO'] = json.loads((out / 'measure-source-io-summary.json').read_text())
        dump(out / 'result.json', result)
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
