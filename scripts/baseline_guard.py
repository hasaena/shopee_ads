from __future__ import annotations

import subprocess
import sys


PROTECTED_PREFIXES = (
    "src/dotori_shopee_automation/shopee/",
    "src/dotori_shopee_automation/token_preflight_gate.py",
    "src/dotori_shopee_automation/webapp.py",
)

REPORT_PREFIXES = (
    "src/dotori_shopee_automation/ads/",
    "src/dotori_shopee_automation/discord_notifier.py",
)

DOC_OVERRIDE_PREFIXES = (
    "docs/",
    "README.md",
    "scripts/",
)


def _staged_files() -> list[str]:
    cp = subprocess.run(
        ["git", "diff", "--cached", "--name-only"],
        capture_output=True,
        text=True,
        check=False,
    )
    if cp.returncode != 0:
        return []
    return [line.strip().replace("\\", "/") for line in cp.stdout.splitlines() if line.strip()]


def _matches(path: str, prefixes: tuple[str, ...]) -> bool:
    return any(path.startswith(prefix) for prefix in prefixes)


def main() -> int:
    staged = _staged_files()
    if not staged:
        return 0

    # Ignore docs/scripts-only commits for this guard.
    code_files = [p for p in staged if not _matches(p, DOC_OVERRIDE_PREFIXES)]
    if not code_files:
        return 0

    touched_protected = [p for p in code_files if _matches(p, PROTECTED_PREFIXES)]
    touched_report = [p for p in code_files if _matches(p, REPORT_PREFIXES)]

    if touched_protected and touched_report:
        print("ERROR: baseline guard blocked this commit.")
        print("Do not mix Shopee auth/token surface and report surface in one commit.")
        print("Protected files:")
        for p in touched_protected:
            print(f" - {p}")
        print("Report files:")
        for p in touched_report:
            print(f" - {p}")
        print("Split into two commits/PRs to reduce production risk.")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
