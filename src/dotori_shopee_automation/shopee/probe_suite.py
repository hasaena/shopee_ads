from __future__ import annotations

from datetime import datetime
from pathlib import Path
from .plan import load_plan
from .plan_runner import run_plan_for_shops
from .probe_analyzer import (
    analyze_artifacts,
    build_discord_summary,
    write_csv_summary,
    write_markdown_summary,
)
from .summary_links import build_summary_ref
from ..discord_notifier import send


def run_probe_suite(
    *,
    settings,
    shops,
    plan_path: Path,
    date_value: str,
    user_vars: dict[str, str],
    save_root: Path,
    out_dir: Path,
    dry_run: bool,
    send_discord: bool,
    channel: str,
    include_schema_hints: bool = False,
    client_factory=None,
) -> dict[str, str | int]:
    plan_def = load_plan(plan_path)
    vars_map = dict(user_vars)
    if "date" not in vars_map:
        vars_map["date"] = _date_from_yyyymmdd(date_value)

    shop_keys = [shop.shop_key for shop in shops]
    print(
        "probe_suite "
        f"plan_path={plan_path} date={date_value} "
        f"shops={','.join(shop_keys) if shop_keys else '-'} save_root={save_root}"
    )

    summary = run_plan_for_shops(
        shops,
        plan_def,
        settings,
        vars_map,
        str(save_root),
        no_print=True,
        continue_on_error=True,
        dry_run=dry_run,
        plan_path=str(plan_path),
        artifact_date=date_value,
        client_factory=client_factory,
    )
    print(
        "plan_summary "
        f"total_calls={summary['total']} ok={summary['ok']} fail={summary['failed']}"
    )

    records = analyze_artifacts(save_root, date_value, shop_keys)
    if not records:
        print(
            f"analyze=no_artifacts save_root={save_root} date={date_value} shops={','.join(shop_keys)}"
        )
        return {
            "total": summary["total"],
            "ok": summary["ok"],
            "fail": summary["failed"],
            "analyzed": 0,
        }

    out_dir.mkdir(parents=True, exist_ok=True)
    md_path = out_dir / f"probe_summary_{date_value}.md"
    csv_path = out_dir / f"probe_summary_{date_value}.csv"
    write_markdown_summary(
        records,
        md_path,
        date_value=date_value,
        save_root=save_root,
        include_schema_hints=include_schema_hints,
    )
    write_csv_summary(records, csv_path)
    print(f"analyze_outputs md={md_path} csv={csv_path}")

    if send_discord:
        discord_channel = "alerts" if channel == "alert" else "report"
        webhook = _resolve_webhook(settings, discord_channel)
        if not webhook:
            print("discord=skipped reason=missing_webhook")
        else:
            summary_ref = build_summary_ref(settings, out_dir, md_path.name)
            message = build_discord_summary(
                records,
                date_value,
                save_root,
                shop_keys,
                summary_ref,
            )
            send(discord_channel, message, webhook_url=webhook)
            print(f"discord=sent channel={discord_channel}")
    else:
        print("discord=skipped reason=disabled")

    return {
        "total": summary["total"],
        "ok": summary["ok"],
        "fail": summary["failed"],
        "analyzed": len(records),
        "md_path": str(md_path),
        "csv_path": str(csv_path),
    }


def _date_from_yyyymmdd(value: str) -> str:
    try:
        parsed = datetime.strptime(value, "%Y%m%d").date()
    except ValueError:
        return value
    return parsed.isoformat()


def _resolve_webhook(settings, channel: str) -> str | None:
    if channel == "report":
        return settings.discord_webhook_report_url
    if channel == "alerts":
        return settings.discord_webhook_alerts_url or settings.discord_webhook_report_url
    return None
