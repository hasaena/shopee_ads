from __future__ import annotations

from datetime import datetime, timezone
from datetime import date as date_cls
from pathlib import Path
import time
from typing import Callable

from ..db import SessionLocal, init_db
from .auth import refresh_access_token
from .client import ShopeeClient
from .plan import build_artifact_path, build_builtin_vars, interpolate_data
from .redact import redact_secrets, redact_text
from .token_store import get_token, needs_refresh, upsert_token


def run_plan_for_shops(
    shops,
    plan_def,
    settings,
    user_vars: dict[str, str],
    save_root: str | None,
    no_print: bool,
    continue_on_error: bool,
    dry_run: bool,
    plan_path: str,
    artifact_date: str | None = None,
    client_factory: Callable[[object], ShopeeClient] | None = None,
) -> dict[str, int]:
    total = 0
    ok_count = 0
    fail_count = 0
    stop_early = False
    failed_calls: list[str] = []
    root = (
        Path(save_root)
        if save_root
        else (Path("collaboration") / "artifacts" / "shopee_api")
    )
    shops_count = len(shops)

    if client_factory is None:
        client_factory = _build_shopee_client

    for shop_cfg in shops:
        if shop_cfg.shopee_shop_id is None:
            raise ValueError(
                f"shopee_shop_id missing in shops config for {shop_cfg.shop_key}"
            )
        vars_map = build_builtin_vars(shop_cfg.shop_key, shop_cfg.shopee_shop_id)
        vars_map.update(user_vars)

        if dry_run:
            for call in plan_def.calls:
                params = interpolate_data(call.params, vars_map)
                _ = interpolate_data(call.body, vars_map) if call.body else None
                api_path = interpolate_data(call.path, vars_map)
                requested_at = _resolve_requested_at(artifact_date)
                preview_path = (
                    build_artifact_path(
                        root,
                        shop_cfg.shop_key,
                        call.name,
                        api_path,
                        requested_at,
                    )
                    if call.save
                    else None
                )
                keys = sorted(params.keys()) if isinstance(params, dict) else []
                preview = (
                    "dry-run "
                    f"shop={shop_cfg.shop_key} call={call.name} method={call.method} "
                    f"path={api_path} params={keys} save={preview_path if preview_path else '-'}"
                )
                print(preview)
                total += 1
                ok_count += 1
            continue

        client = client_factory(settings)
        init_db()
        session = SessionLocal()
        try:
            token = get_token(session, shop_cfg.shop_key)
            if token is None:
                raise RuntimeError("no token found; run shopee exchange-code first")

            for call in plan_def.calls:
                if needs_refresh(token.access_token_expires_at):
                    refreshed = refresh_access_token(
                        client,
                        settings.shopee_partner_id,
                        settings.shopee_partner_key,
                        shop_cfg.shopee_shop_id,
                        token.refresh_token,
                        int(datetime.now().timestamp()),
                    )
                    upsert_token(
                        session,
                        shop_cfg.shop_key,
                        refreshed.shop_id,
                        refreshed.access_token,
                        refreshed.refresh_token,
                        refreshed.access_expires_at,
                    )
                    session.commit()
                    token = get_token(session, shop_cfg.shop_key)

                params = interpolate_data(call.params, vars_map)
                body = interpolate_data(call.body, vars_map) if call.body else None
                api_path = interpolate_data(call.path, vars_map)

                requested_at = _resolve_requested_at(artifact_date)
                start = time.monotonic()
                response = None
                ok = True
                error_text = None
                http_status = None
                shopee_error = None

                try:
                    response = client.request(
                        call.method,
                        api_path,
                        shop_id=shop_cfg.shopee_shop_id,
                        access_token=token.access_token,
                        params=params or None,
                        json=body,
                    )
                except Exception as exc:  # noqa: BLE001
                    ok = False
                    if hasattr(exc, "response") and exc.response is not None:
                        http_status = getattr(exc.response, "status_code", None)
                        if http_status:
                            error_text = f"HTTP {http_status}"
                    if not error_text:
                        error_text = str(exc)

                duration_ms = int((time.monotonic() - start) * 1000)

                if ok and isinstance(response, dict):
                    shopee_error = response.get("error")
                    if shopee_error not in (None, 0):
                        ok = False
                        message = response.get("message") or response.get("msg") or "-"
                        error_text = f"Shopee API error {shopee_error}: {message}"

                meta = {
                    "shop_key": shop_cfg.shop_key,
                    "shop_id": shop_cfg.shopee_shop_id,
                    "plan_name": plan_def.name,
                    "call_name": call.name,
                    "method": call.method,
                    "path": api_path,
                    "params": params,
                    "body": body,
                    "requested_at": requested_at.isoformat(),
                    "duration_ms": duration_ms,
                    "http_status": http_status,
                    "shopee_error": shopee_error,
                }
                meta = redact_secrets(
                    meta,
                    extra_keys={
                        "partner_key",
                        "access_token",
                        "refresh_token",
                        "sign",
                        "authorization",
                        "cookie",
                        "secret",
                        "client_secret",
                    },
                )

                saved_path = None
                if call.save:
                    saved_path = build_artifact_path(
                        root,
                        shop_cfg.shop_key,
                        call.name,
                        api_path,
                        requested_at,
                    )
                    saved_path.parent.mkdir(parents=True, exist_ok=True)
                    if ok and response is not None:
                        redacted_response = redact_secrets(response)
                        payload = {"__meta": meta}
                        if isinstance(redacted_response, dict):
                            payload.update(redacted_response)
                        else:
                            payload["response"] = redacted_response
                    else:
                        payload = {
                            "__meta": meta,
                            "error": redact_text(error_text or "unknown error"),
                        }
                    _write_json(saved_path, payload, pretty=False)

                summary = f"shop={shop_cfg.shop_key} call={call.name} ok={1 if ok else 0}"
                if not ok and error_text:
                    summary += f" error={redact_text(error_text)}"
                summary += f" saved={saved_path if saved_path else '-'}"
                summary += f" duration_ms={duration_ms}"
                print(summary)

                if ok and not no_print and response is not None:
                    print(_dump_json(redact_secrets(response), pretty=True))

                total += 1
                if ok:
                    ok_count += 1
                else:
                    fail_count += 1
                    failed_calls.append(call.name)
                    if not continue_on_error:
                        stop_early = True
                        break
            if stop_early:
                break
        finally:
            session.close()
        if stop_early:
            break

    print(
        f"total_calls={total} ok={ok_count} fail={fail_count} "
        f"save_root={root} shops_count={shops_count} plan_path={plan_path}"
    )
    if fail_count:
        shown = failed_calls[:20]
        print(f"failed_calls={','.join(shown)}")
    return {"total": total, "ok": ok_count, "failed": fail_count, "root": str(root)}


def _build_shopee_client(settings) -> ShopeeClient:
    return ShopeeClient(
        partner_id=settings.shopee_partner_id,
        partner_key=settings.shopee_partner_key,
        host=settings.shopee_api_host,
    )


def _resolve_requested_at(artifact_date: str | None) -> datetime:
    now = datetime.now(timezone.utc)
    if not artifact_date:
        return now
    try:
        if len(artifact_date) == 8 and artifact_date.isdigit():
            parsed = datetime.strptime(artifact_date, "%Y%m%d").date()
        else:
            parsed = date_cls.fromisoformat(artifact_date)
    except ValueError:
        return now
    return datetime.combine(parsed, now.time(), tzinfo=timezone.utc)


def _write_json(path: Path, payload: dict, pretty: bool = False) -> None:
    path.write_text(_dump_json(payload, pretty=pretty), encoding="utf-8")


def _dump_json(payload: dict, pretty: bool = False) -> str:
    import json

    if pretty:
        return json.dumps(payload, ensure_ascii=True, indent=2)
    return json.dumps(payload, ensure_ascii=True)
