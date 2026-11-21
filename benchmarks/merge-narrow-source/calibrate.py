"""Freeze a disk bound from a passing current-SDK local 2048-file N pilot."""
import argparse
import json
import shutil
from pathlib import Path

from spec import UPDATE_NAMES, dump, require, sha

HERE = Path(__file__).resolve().parent


def main():
    p = argparse.ArgumentParser(__doc__)
    p.add_argument('--root', type=Path, required=True)
    p.add_argument('--name', required=True)
    p.add_argument('--output', type=Path, default=HERE / 'calibration.json')
    a = p.parse_args()
    require(Path(a.name).name == a.name and a.name not in ('.', '..'), 'unsafe pilot name')
    output = a.output.resolve()
    require(output.is_relative_to(HERE) and not output.exists(), 'new calibration must stay under bench')
    out = a.root.resolve() / a.name
    result = json.loads((out / 'result.json').read_text())
    c = result['case']
    require(c['profile'] == 'local' and c['targetFiles'] == 2048 and c['rowsPerFile'] == 2
            and c['variant'] == 'insert17' and result['mode'] == 'N', 'use the full local N pilot')
    require(result['checks'] and all(result['checks'].values()), 'pilot validation failed')
    layout = json.loads((out / 'file-layout.json').read_text())
    partial = [f for f in layout['newFiles'] if f['firstRowId'] < c['oldRows']]
    require(len(partial) == c['matched'] == 2048 and all(f['rows'] == 2 and
            set(f['writeCols']) == set(UPDATE_NAMES) and f['actualBytes'] == f['bytes'] for f in partial), 'partial file proof incomplete')
    observed = max(f['actualBytes'] for f in partial)
    upper = (((observed * 5 + 3) // 4 + 4095) // 4096) * 4096
    binding = json.loads((a.root / 'binary-binding.json').read_text())
    evidence = output.with_name(output.stem + '-evidence')
    evidence.mkdir()
    leaves = []
    for name, src in [('binary-binding.json', a.root / 'binary-binding.json')] + [
            (n, out / n) for n in ('result.json', 'file-layout.json', 'sql.txt', 'source.sql',
                                    'source-dataset.json', 'source-footers.json', 'source-fairness.json',
                                    'prepare-source-io-summary.json', 'measure-source-io-summary.json')]:
        dst = evidence / name
        shutil.copyfile(str(src), str(dst))
        leaves.append(dict(path=dst.relative_to(output.parent).as_posix(), sha256=sha(dst)))
    dump(output, dict(sdkSHA256=binding['sdk']['sha256'], helpersSHA256=binding['helpers']['sha256'],
         overlaySHA256=binding.get('overlay', {}).get('sha256'), pilotFiles=2048, rowsPerFile=2,
         columns=259, updateColumns=128, writeCols=UPDATE_NAMES, sourceProfile=c,
         sourceDatasetSHA256=sha(out / 'source-dataset.json'), sqlSHA256=sha(out / 'sql.txt'),
         includesFooter=True, observedMaxPartialFileBytes=observed, upperPartialFileBytes=upper,
         formula='round_up(maxObservedPartialFileBytes * 1.25, 4096)', probeEvidence=leaves,
         appliesTo='all cloud F/R/N cases: UPDATE128 two-row partial files; variant row counts remain dynamic'))
    print(str(output))


if __name__ == '__main__':
    main()
