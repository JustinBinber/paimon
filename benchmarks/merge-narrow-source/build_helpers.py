"""Compile bench-only Java 8 helpers against the final SDK and package its service entry."""
import argparse
import os
import shutil
import subprocess
from pathlib import Path

from spec import dump, require, sha

HERE = Path(__file__).resolve().parent


def main():
    p = argparse.ArgumentParser(__doc__)
    p.add_argument('--sdk', type=Path, required=True)
    p.add_argument('--spark', type=Path, required=True)
    p.add_argument('--java-home', type=Path, required=True)
    p.add_argument('--output', type=Path, default=HERE / 'helpers.jar')
    a = p.parse_args()
    output = a.output.resolve()
    require(output.is_relative_to(HERE) and not output.exists(), 'new helper output must be under bench')
    sdk_hash = sha(a.sdk)
    build = HERE / (output.stem + '-build-' + sdk_hash[:12])
    build.mkdir()
    classes = build / 'classes'
    classes.mkdir()
    java = a.java_home / 'bin'
    suffix = '.exe' if os.name == 'nt' else ''
    sources = [HERE / name for name in ('NarrowMetrics.java', 'NarrowInspect.java', 'CountingCatalogFactory.java')]
    cp = str(a.sdk.resolve()) + os.pathsep + str(a.spark.resolve() / 'jars' / '*')
    command = [str(java / ('javac' + suffix)), '-source', '8', '-target', '8', '-cp', cp,
               '-d', str(classes)] + [str(s) for s in sources]
    with (build / 'compile.log').open('xb') as log:
        subprocess.run(command, stdout=log, stderr=subprocess.STDOUT, check=True)
    services = [Path('META-INF/services') / name for name in
                ('org.apache.paimon.catalog.CatalogFactory', 'org.apache.paimon.factories.Factory')]
    for service in services:
        (classes / service).parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(str(HERE / service), str(classes / service))
    subprocess.run([str(java / ('jar' + suffix)), 'cf', str(output), '-C', str(classes), '.'], check=True)
    dump(output.with_suffix('.binding.json'), dict(sdkSHA256=sdk_hash, helpersSHA256=sha(output),
         sources={s.name: sha(s) for s in sources}, services={str(s): sha(HERE / s) for s in services},
         javaSourceTarget='8', catalog='bench-counted'))


if __name__ == '__main__':
    main()
