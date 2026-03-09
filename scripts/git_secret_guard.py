from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path


PATTERNS: list[tuple[str, re.Pattern[str]]] = [
    ("discord_webhook", re.compile(r"https://discord\.com/api/webhooks/\d+/[A-Za-z0-9_\-]+")),
    ("shopee_partner_key", re.compile(r"SHOPEE_PARTNER_KEY\s*=\s*[A-Za-z0-9]{32,}")),
    ("dotori_ops_token", re.compile(r"DOTORI_OPS_TOKEN\s*=\s*[A-Za-z0-9_\-]{24,}")),
]


def _git_staged_files() -> list[Path]:
    cp = subprocess.run(
        ["git", "diff", "--cached", "--name-only"],
        capture_output=True,
        text=True,
        check=False,
    )
    if cp.returncode != 0:
        return []
    files: list[Path] = []
    for line in cp.stdout.splitlines():
        path = Path(line.strip())
        if path.exists() and path.is_file():
            files.append(path)
    return files


def main() -> int:
    staged = _git_staged_files()
    if not staged:
        return 0

    blocked: list[str] = []
    for path in staged:
        try:
            text = path.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            continue
        for name, pattern in PATTERNS:
            if pattern.search(text):
                blocked.append(f"{path} ({name})")
                break

    if blocked:
        print("ERROR: secret-like values detected in staged files:")
        for row in blocked:
            print(f" - {row}")
        print("Commit blocked. Remove/redact secrets first.")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

