from __future__ import annotations

import hashlib
from collections import defaultdict
from pathlib import Path


RUNTIME_PATHS = [
    Path(".venv"),
    Path(".pytest_cache"),
    Path("artifacts"),
    Path("reports"),
    Path("collaboration"),
    Path("dotori.db"),
    Path("shopee_tokens_export.json"),
]


def _hash_file(path: Path) -> str:
    h = hashlib.sha256()
    h.update(path.read_bytes())
    return h.hexdigest()


def _duplicate_fixtures(root: Path) -> list[list[Path]]:
    by_hash: dict[str, list[Path]] = defaultdict(list)
    if not root.exists():
        return []
    for p in root.rglob("*"):
        if p.is_file():
            by_hash[_hash_file(p)].append(p)
    groups = [paths for paths in by_hash.values() if len(paths) > 1]
    groups.sort(key=lambda g: (len(g), g[0].as_posix()), reverse=True)
    return groups


def main() -> int:
    print("repo_hygiene_audit_start=1")

    runtime_found = [p for p in RUNTIME_PATHS if p.exists()]
    print(f"runtime_paths_found={len(runtime_found)}")
    for p in runtime_found:
        print(f"runtime_path={p.as_posix()}")

    dup_groups = _duplicate_fixtures(Path("tests/fixtures"))
    print(f"fixture_duplicate_groups={len(dup_groups)}")
    for group in dup_groups[:20]:
        print("---")
        for p in group:
            print(f"dup={p.as_posix()}")

    print("repo_hygiene_audit_done=1")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
