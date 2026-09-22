#!/usr/bin/env python3
"""Create a cleaned zip of the repo honoring .funcignore patterns.

Usage: python scripts/pack_app.py -d dest.zip [--dry-run]
"""
import argparse
import fnmatch
import sys
from pathlib import Path
from zipfile import ZipFile, ZIP_DEFLATED


def read_funcignore(root: Path):
    p = root / '.funcignore'
    patterns = []
    if not p.exists():
        return patterns
    for line in p.read_text(encoding='utf-8').splitlines():
        s = line.strip()
        if not s or s.startswith('#'):
            continue
        patterns.append(s)
    return patterns


def matches_any(rel_posix: str, patterns):
    # Try several matching strategies for each pattern
    for pat in patterns:
        # direct fnmatch against the path
        if fnmatch.fnmatch(rel_posix, pat):
            return True
        # match with recursive wildcard
        if fnmatch.fnmatch(rel_posix, pat.lstrip('./')):
            return True
        # if pattern refers to a dir, match prefix
        if pat.endswith('/') and rel_posix.startswith(pat.rstrip('/')):
            return True
        # allow matching top-level names
        if fnmatch.fnmatch(Path(rel_posix).name, pat):
            return True
    return False


def build_zip(root: Path, dest: Path, dry_run=False):
    patterns = read_funcignore(root)
    included = []
    excluded = []

    for p in root.rglob('*'):
        if p.is_dir():
            continue
        rel = p.relative_to(root)
        rel_posix = rel.as_posix()
        if matches_any(rel_posix, patterns):
            excluded.append(rel_posix)
            continue
        included.append((p, rel_posix))

    if dry_run:
        print(f"Dry run: {len(included)} files would be added, {len(excluded)} excluded")
        if included:
            print('\nIncluded samples:')
            for f, r in included[:20]:
                print('  ', r)
        if excluded:
            print('\nExcluded samples:')
            for r in excluded[:20]:
                print('  ', r)
        return 0

    dest.parent.mkdir(parents=True, exist_ok=True)
    with ZipFile(dest, 'w', ZIP_DEFLATED) as z:
        for p, arc in included:
            z.write(p, arcname=arc)

    print(f'Created {dest} with {len(included)} files (excluded {len(excluded)})')
    return 0


def main(argv):
    ap = argparse.ArgumentParser()
    ap.add_argument('-d', '--destination', required=True, help='Destination zip path')
    ap.add_argument('--dry-run', action='store_true')
    args = ap.parse_args(argv)
    root = Path.cwd()
    dest = Path(args.destination)
    return build_zip(root, dest, dry_run=args.dry_run)


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
