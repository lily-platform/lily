#!/usr/bin/env python3
"""Check publishable internal dependencies and print their publication layers.

Includes optional, target-specific and development dependencies. External crate
availability and archive verification are separate release checks.
"""
import argparse
from graphlib import CycleError, TopologicalSorter
import json
from pathlib import Path
import sys
import tomllib

ROOT = Path(__file__).resolve().parents[2]
KINDS = ('dependencies', 'build-dependencies', 'dev-dependencies')


def manifest(path):
    return tomllib.loads(path.read_text())


def dependency_table(value):
    return {'version': value} if isinstance(value, str) else dict(value)


def audit(root):
    workspace = manifest(root / 'Cargo.toml')['workspace']
    packages = {}
    for pattern in workspace['members']:
        for directory in sorted(root.glob(pattern)):
            document = manifest(directory / 'Cargo.toml')
            package = document['package']
            if package.get('publish') is False:
                continue
            name = package['name']
            if name in packages:
                raise ValueError(f'duplicate workspace package: {name}')
            packages[name] = (directory, document)
    by_path = {directory.resolve(): name for name, (directory, _) in packages.items()}
    errors, edges = [], []
    inherited_dependencies = workspace.get('dependencies', {})
    for name, (directory, document) in sorted(packages.items()):
        tables = [('', document), *[(f'target.{key}.', value) for key, value in document.get('target', {}).items()]]
        for scope, table in tables:
            for kind in KINDS:
                for alias, value in sorted(table.get(kind, {}).items()):
                    entry = dependency_table(value)
                    base = directory
                    location = f'{name}: {scope}{kind}.{alias}'
                    if entry.get('workspace'):
                        if alias not in inherited_dependencies:
                            errors.append(f'{location}: missing workspace dependency')
                            continue
                        entry = {**dependency_table(inherited_dependencies[alias]), **entry}
                        base = root
                    dependency = entry.get('package', alias)
                    path = (base / entry['path']).resolve() if 'path' in entry else None
                    if path in by_path and dependency != by_path[path]:
                        errors.append(f'{location}: package name does not match {path}')
                    if dependency not in packages:
                        if path is not None:
                            errors.append(f'{location}: path dependency is outside the publishable workspace')
                        continue
                    if path != packages[dependency][0].resolve():
                        errors.append(f'{location}: expected path to {dependency}')
                    requirement = entry.get('version')
                    if not isinstance(requirement, str) or not requirement.strip() or '*' in requirement:
                        errors.append(f'{location}: a registry version is required alongside path')
                    if entry.get('git') or entry.get('registry'):
                        errors.append(f'{location}: internal releases must resolve through crates.io')
                    edges.append({'package': name, 'dependency': dependency, 'kind': kind,
                                  'target': scope.removeprefix('target.').removesuffix('.'),
                                  'alias': alias, 'version': requirement,
                                  'optional': entry.get('optional', False)})
    if errors:
        raise ValueError('\n'.join(errors))
    graph = {name: set() for name in packages}
    for edge in edges:
        graph[edge['package']].add(edge['dependency'])
    sorter = TopologicalSorter(graph)
    try:
        sorter.prepare()
    except CycleError as error:
        raise ValueError('internal publication cycle: ' + ' -> '.join(error.args[1])) from error
    layers = []
    while sorter.is_active():
        ready = sorted(sorter.get_ready())
        layers.append(ready)
        sorter.done(*ready)
    return {'package_count': len(packages), 'internal_dependency_count': len(edges),
            'includes': list(KINDS), 'all_features_and_targets': True,
            'publication_layers': layers, 'dependencies': edges}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--json', action='store_true', help='Emit the audited graph and publication layers as JSON')
    args = parser.parse_args()
    try:
        result = audit(ROOT)
    except (ValueError, KeyError, OSError, tomllib.TOMLDecodeError) as error:
        print(f'FAIL package dependencies: {error}', file=sys.stderr)
        return 1
    if args.json:
        print(json.dumps(result, indent=2))
    else:
        print(f'PASS {result["package_count"]} packages, {result["internal_dependency_count"]} internal dependencies: path + version, no publication cycles')
        for index, layer in enumerate(result['publication_layers'], start=1):
            print(f'{index}: ' + ', '.join(layer))
    return 0


if __name__ == '__main__':
    sys.exit(main())
