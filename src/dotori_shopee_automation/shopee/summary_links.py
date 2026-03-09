from __future__ import annotations

from pathlib import Path


def build_summary_ref(settings, out_root: Path, filename: str) -> str:
    base_url = settings.report_base_url
    if base_url:
        reports_root = Path(settings.reports_dir).resolve()
        try:
            rel = out_root.resolve().relative_to(reports_root)
        except ValueError:
            return str(out_root / filename)
        rel_path = (Path("reports") / rel / filename).as_posix()
        return f"{base_url.rstrip('/')}/{rel_path}"
    return str(out_root / filename)
