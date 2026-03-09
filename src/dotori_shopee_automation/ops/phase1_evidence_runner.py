from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ..ads.campaign_probe import _default_rate_limit_state_path, run_campaign_probe
from ..config import get_settings, load_shops
from ..utils.envfile import load_env_file


def _read_json(path: Path) -> dict[str, Any] | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:  # noqa: BLE001
        return None
    if not isinstance(payload, dict):
        return None
    return payload


def _extract_trace(payload: dict[str, Any]) -> dict[str, Any]:
    trace = payload.get("__trace")
    if isinstance(trace, dict):
        return trace
    return {}


def _extract_request_id(payload: dict[str, Any], trace: dict[str, Any]) -> str | None:
    request_id = trace.get("request_id")
    if request_id not in (None, ""):
        return str(request_id)
    for key in ("request_id", "requestId"):
        value = payload.get(key)
        if value not in (None, ""):
            return str(value)
    return None


def _build_rate_limit_summary(
    raw_root: Path,
    *,
    max_rows: int = 50,
    cooldown_state_path: Path | None = None,
) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    if not raw_root.exists():
        return {"rows": rows, "http_status_distribution": {}, "retry_after_seen": 0}

    for file_path in sorted(raw_root.rglob("*.json")):
        payload = _read_json(file_path)
        if not isinstance(payload, dict):
            continue
        trace = _extract_trace(payload)
        if not trace:
            continue
        endpoint = str(trace.get("path") or "-")
        timestamp = str(trace.get("called_at_utc") or "-")
        http_status = trace.get("http_status")
        api_error = trace.get("api_error")
        api_message = trace.get("api_message")
        retry_after = trace.get("retry_after_sec")
        request_id = _extract_request_id(payload, trace)
        row = {
            "file": str(file_path),
            "endpoint": endpoint,
            "http_status": http_status,
            "api_error": api_error,
            "api_message": api_message,
            "request_id": request_id,
            "retry_after_sec": retry_after,
            "timestamp_utc": timestamp,
            "rate_limited": int(trace.get("rate_limited") or 0),
            "skipped_by_cooldown": int(trace.get("skipped_by_cooldown") or 0),
            "skipped_by_budget": int(trace.get("skipped_by_budget") or 0),
        }
        rows.append(row)

    rows.sort(key=lambda r: str(r.get("timestamp_utc") or ""), reverse=True)
    rows = rows[:max_rows]

    status_dist: dict[str, int] = {}
    api_error_dist: dict[str, int] = {}
    retry_seen = 0
    for row in rows:
        key = str(row.get("http_status"))
        status_dist[key] = status_dist.get(key, 0) + 1
        err_key = str(row.get("api_error") or "-")
        api_error_dist[err_key] = api_error_dist.get(err_key, 0) + 1
        if row.get("retry_after_sec") not in (None, "", 0):
            retry_seen += 1
    return {
        "rows": rows,
        "http_status_distribution": status_dist,
        "api_error_distribution": api_error_dist,
        "retry_after_seen": 1 if retry_seen > 0 else 0,
        "source": str(raw_root),
        "cooldown_state_path": str(cooldown_state_path) if cooldown_state_path else "",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
    }


def _collect_probe_payloads(raw_shop_dir: Path, pattern: str, limit: int = 5) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for path in sorted(raw_shop_dir.glob(pattern))[:limit]:
        payload = _read_json(path)
        if not isinstance(payload, dict):
            continue
        out.append(payload)
    return out


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _build_verdict_md(
    *,
    output_path: Path,
    shop_results: list[Any],
    rate_summary: dict[str, Any],
) -> None:
    lines: list[str] = []
    lines.append("# verdict")
    lines.append("")
    lines.append("## Product-level campaign meta")
    for shop in shop_results:
        registry = list(getattr(shop, "registry_rows", []) or [])
        preflight_ok = bool(getattr(shop, "preflight_ok", True))
        preflight_reason = str(getattr(shop, "preflight_reason", "") or "")
        meta_probe_ok = bool(getattr(shop, "meta_probe_ok", False))
        meta_probe_reason = str(getattr(shop, "meta_probe_reason", "") or "")
        if not preflight_ok:
            reason = preflight_reason or "token_invalid"
            ad_name_state = f"unknown (reason={reason})"
            budget_state = f"unknown (reason={reason})"
            item_state = f"unknown (reason={reason})"
        elif not meta_probe_ok:
            reason = meta_probe_reason or "meta_unavailable"
            ad_name_state = f"unknown (reason={reason})"
            budget_state = f"unknown (reason={reason})"
            item_state = f"unknown (reason={reason})"
        else:
            ad_name_present = any(str(row.get("ad_name") or "").strip() for row in registry)
            budget_present = any(
                str(row.get("daily_budget") or "").strip()
                or str(row.get("total_budget") or "").strip()
                for row in registry
            )
            item_list_present = any(int(row.get("item_count") or 0) > 0 for row in registry)
            ad_name_state = "yes" if ad_name_present else "no"
            budget_state = "yes" if budget_present else "no"
            item_state = "yes" if item_list_present else "no"
        lines.append(f"- shop={shop.shop_key}: ad_name={ad_name_state}")
        lines.append(f"- shop={shop.shop_key}: campaign_budget={budget_state}")
        lines.append(f"- shop={shop.shop_key}: item_id_list={item_state}")
    lines.append("")
    lines.append("## GMS")
    for shop in shop_results:
        preflight_ok = bool(getattr(shop, "preflight_ok", True))
        preflight_reason = str(getattr(shop, "preflight_reason", "") or "")
        gms_ok = bool(getattr(shop, "gms_ok", False))
        gms_reason = str(getattr(shop, "gms_probe_reason", "") or "")
        gms_campaign_ids = list(getattr(shop, "gms_campaign_ids", set()) or set())
        if not preflight_ok:
            campaign_id_state = f"unknown (reason={preflight_reason or 'preflight_failed'})"
        elif not gms_ok:
            campaign_id_state = f"unknown (reason={gms_reason or 'gms_unavailable'})"
        else:
            campaign_id_state = "yes" if len(gms_campaign_ids) > 0 else "no"
        lines.append(f"- shop={shop.shop_key}: campaign_id={campaign_id_state}")
        lines.append("- budget/name field present in API response? no")
    lines.append("")
    lines.append("## Rate limit")
    status_dist = rate_summary.get("http_status_distribution") or {}
    api_error_dist = rate_summary.get("api_error_distribution") or {}
    cooldown_state_path = str(rate_summary.get("cooldown_state_path") or "").strip()
    lines.append(f"- http status distribution: {json.dumps(status_dist, ensure_ascii=True)}")
    lines.append(f"- api_error distribution: {json.dumps(api_error_dist, ensure_ascii=True)}")
    lines.append(f"- Retry-After seen? {'yes' if int(rate_summary.get('retry_after_seen') or 0) == 1 else 'no'}")
    lines.append(f"- cooldown_state_path={cooldown_state_path}")
    lines.append(
        "- recommended cooldown policy: "
        "respect Retry-After; if absent use 60m default with exponential backoff (max 6h); "
        "skip network calls while cooldown is active."
    )
    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _select_target_shops(only_shops: str) -> list[Any]:
    keys = [x.strip() for x in str(only_shops or "").split(",") if x.strip()]
    shops = [shop for shop in load_shops() if shop.enabled]
    if keys:
        keyset = set(keys)
        shops = [shop for shop in shops if shop.shop_key in keyset]
    return shops


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Phase1 ads evidence runner")
    parser.add_argument("--only-shops", default="samord,minmin")
    parser.add_argument("--include", default="campaign_meta,gms_performance")
    parser.add_argument("--artifacts-dir", default="./artifacts/ads_rate_limit")
    parser.add_argument("--redact", action="store_true")
    parser.add_argument("--max-requests", type=int, default=30)
    parser.add_argument("--env-file", default=None)
    parser.add_argument("--days", type=int, default=7)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--ignore-cooldown", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.env_file:
        loaded = load_env_file(str(args.env_file), override=False)
        print(f"env_file_loaded path={args.env_file} keys={len(loaded)}")

    include = {x.strip() for x in str(args.include or "").split(",") if x.strip()}
    if "campaign_meta" not in include and "gms_performance" not in include:
        print("evidence_runner_ok=0 reason=empty_include")
        return 1
    if args.max_requests < 1:
        print("evidence_runner_ok=0 reason=max_requests_invalid")
        return 1

    settings = get_settings()
    mode_value = "dry-run" if bool(args.dry_run) else "live"
    if mode_value == "live" and (settings.shopee_partner_id is None or not settings.shopee_partner_key):
        print("evidence_runner_ok=0 reason=missing_shopee_partner_settings")
        return 1

    target_shops = _select_target_shops(args.only_shops)
    if not target_shops:
        print("evidence_runner_ok=0 reason=no_target_shops")
        return 1
    for shop in target_shops:
        if shop.shopee_shop_id is None:
            print(f"evidence_runner_ok=0 reason=missing_shop_id shop={shop.shop_key}")
            return 1

    artifacts_dir = Path(args.artifacts_dir).resolve()
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    probe_dir = artifacts_dir / f"probe_{ts}"
    cooldown_state_dst = artifacts_dir / "rate_limit_state.json"
    print(
        "evidence_runner_start "
        f"shops={','.join([s.shop_key for s in target_shops])} "
        f"include={','.join(sorted(include))} mode={mode_value} max_requests={args.max_requests} "
        f"artifacts_dir={artifacts_dir}"
    )

    probe_result = run_campaign_probe(
        settings=settings,
        target_shops=target_shops,
        mode=mode_value,
        days=max(int(args.days), 1),
        out_dir=probe_dir,
        redact=bool(args.redact),
        fixture_payload=None,
        max_requests_per_shop=int(args.max_requests),
        sync_db=True,
        ignore_cooldown=bool(args.ignore_cooldown),
    )
    cooldown_state_src = Path(
        str(probe_result.get("rate_limit_state_path") or _default_rate_limit_state_path())
    ).resolve()
    shop_results = list(probe_result.get("shop_results") or [])
    raw_root = probe_dir / "raw"

    for shop in shop_results:
        shop_key = shop.shop_key
        shop_raw = raw_root / shop_key
        campaign_payload = {
            "shop_key": shop_key,
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "id_list_count": int(getattr(shop, "id_list_count", 0)),
            "setting_rows_raw": int(getattr(shop, "setting_rows_raw", 0)),
            "setting_chunks_ok": int(getattr(shop, "setting_chunks_ok", 0)),
            "setting_chunks_fail": int(getattr(shop, "setting_chunks_fail", 0)),
            "campaign_id_list_pages": _collect_probe_payloads(shop_raw, "campaign_id_list_page_*.json"),
            "setting_info_chunks": _collect_probe_payloads(shop_raw, "setting_info_chunk_*.json"),
        }
        gms_payload = {
            "shop_key": shop_key,
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "gms_ok": 1 if bool(getattr(shop, "gms_ok", False)) else 0,
            "gms_campaign_ids": sorted([str(x) for x in getattr(shop, "gms_campaign_ids", set())]),
            "gms_campaign_performance_tries": _collect_probe_payloads(
                shop_raw, "gms_campaign_performance_try_*.json"
            ),
        }
        _write_json(artifacts_dir / f"campaign_meta_probe_{shop_key}_{ts}.json", campaign_payload)
        _write_json(artifacts_dir / f"gms_probe_{shop_key}_{ts}.json", gms_payload)
        preflight_payload = {
            "shop_key": shop_key,
            "endpoint": str(getattr(shop, "preflight_endpoint", "/api/v2/ads/get_total_balance")),
            "http_status": getattr(shop, "preflight_http_status", None),
            "api_error": getattr(shop, "preflight_api_error", None),
            "api_message": getattr(shop, "preflight_api_message", None),
            "request_id": getattr(shop, "preflight_request_id", None),
            "token_len": int(getattr(shop, "token_len", 0) or 0),
            "token_sha8": str(getattr(shop, "token_sha8", "") or ""),
            "ok": 1 if bool(getattr(shop, "preflight_ok", True)) else 0,
            "reason": str(getattr(shop, "preflight_reason", "") or ""),
        }
        _write_json(artifacts_dir / f"preflight_{shop_key}_{ts}.json", preflight_payload)

    try:
        cooldown_state_dst.parent.mkdir(parents=True, exist_ok=True)
        if cooldown_state_src.exists():
            cooldown_state_dst.write_text(
                cooldown_state_src.read_text(encoding="utf-8"),
                encoding="utf-8",
            )
        else:
            cooldown_state_dst.write_text(
                json.dumps({"shops": {}, "generated_at_utc": datetime.now(timezone.utc).isoformat()}, ensure_ascii=True, indent=2),
                encoding="utf-8",
            )
    except Exception:  # noqa: BLE001
        pass

    rate_summary = _build_rate_limit_summary(
        raw_root,
        max_rows=50,
        cooldown_state_path=cooldown_state_dst if cooldown_state_dst.exists() else cooldown_state_src,
    )
    _write_json(artifacts_dir / "rate_limit_summary.json", rate_summary)

    verdict_path = artifacts_dir / "verdict.md"
    _build_verdict_md(output_path=verdict_path, shop_results=shop_results, rate_summary=rate_summary)

    print(f"campaign_registry_csv={probe_result.get('registry_csv')}")
    print(f"campaign_probe_summary_md={probe_result.get('summary_md')}")
    print(f"rate_limit_summary={artifacts_dir / 'rate_limit_summary.json'}")
    print(f"verdict_md={verdict_path}")
    print("evidence_runner_ok=1")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
