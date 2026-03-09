from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path


SECRET_KEYS = {
    "access_token",
    "refresh_token",
    "partner_key",
    "authorization",
    "cookie",
    "sign",
    "secret",
    "client_secret",
}

REDACTED_MARKERS = {
    "***",
    "<redacted>",
    "redacted",
    "replaced",
    "masked",
    "-",
    "0",
    "1",
    "true",
    "false",
    "yes",
    "no",
    "present",
    "missing",
    "configured",
}

TEXT_PATTERN = re.compile(
    r"(?i)\b(access_token|refresh_token|partner_key|authorization|cookie|sign|client_secret|secret)\b\s*[:=]\s*([^\s\"']+|\"[^\"]*\"|'[^']*')"
)


def _is_redacted(value: object) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        text = value.strip().strip('"').strip("'")
        if not text:
            return True
        if text.lower() in REDACTED_MARKERS:
            return True
        if "***" in text:
            return True
        if "redacted" in text.lower():
            return True
        return False
    return False


def _scan_json(obj: object, findings: list[tuple[Path, str, str]], path: Path) -> None:
    if isinstance(obj, dict):
        for key, value in obj.items():
            key_text = str(key).lower()
            if key_text in SECRET_KEYS:
                if not _is_redacted(value):
                    findings.append((path, str(key), "non_redacted"))
            _scan_json(value, findings, path)
    elif isinstance(obj, list):
        for item in obj:
            _scan_json(item, findings, path)


def _scan_text(content: str, findings: list[tuple[Path, str, str]], path: Path) -> None:
    for match in TEXT_PATTERN.finditer(content):
        key = match.group(1)
        raw_value = match.group(2)
        value = raw_value.strip().strip('"').strip("'")
        if not _is_redacted(value):
            findings.append((path, key, "non_redacted_text"))


def _should_scan(path: Path) -> bool:
    if path.is_dir():
        return False
    if path.suffix.lower() in {".json", ".md", ".txt", ".log"}:
        return True
    return False


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Scan artifacts/results/probes for non-redacted secrets."
    )
    parser.add_argument(
        "--roots",
        nargs="*",
        default=[
            "collaboration/artifacts",
            "collaboration/results",
            "collaboration/probes",
        ],
        help="Root directories to scan",
    )
    args = parser.parse_args()

    findings: list[tuple[Path, str, str]] = []
    files_scanned = 0

    for root in args.roots:
        root_path = Path(root)
        if not root_path.exists():
            continue
        for path in root_path.rglob("*"):
            if not _should_scan(path):
                continue
            try:
                content = path.read_text(encoding="utf-8")
            except Exception:
                continue
            files_scanned += 1
            if path.suffix.lower() == ".json":
                try:
                    payload = json.loads(content)
                except json.JSONDecodeError:
                    _scan_text(content, findings, path)
                    continue
                _scan_json(payload, findings, path)
            else:
                _scan_text(content, findings, path)

    if findings:
        print(f"security_scan_ok=0 findings={len(findings)} files_scanned={files_scanned}")
        for path, key, reason in findings:
            print(f"file={path} key={key} reason={reason}")
        return 1

    print(f"security_scan_ok=1 files_scanned={files_scanned}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
