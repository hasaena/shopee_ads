from __future__ import annotations

from pathlib import Path
import csv
import json
from types import SimpleNamespace

import httpx

from dotori_shopee_automation.cli import ops_phase1_ads_campaign_probe
from dotori_shopee_automation.ads import campaign_probe as campaign_probe_mod
from dotori_shopee_automation.ads.campaign_probe import _call_live, run_campaign_probe
from dotori_shopee_automation.config import get_settings
from dotori_shopee_automation.shopee.client import ShopeeClient


def _write_shops(path: Path) -> None:
    path.write_text(
        "\n".join(
            [
                "- shop_key: samord",
                "  label: SAMORD",
                "  enabled: true",
                "  shopee_shop_id: 497412318",
                "- shop_key: minmin",
                "  label: MINMIN",
                "  enabled: true",
                "  shopee_shop_id: 567655304",
            ]
        ),
        encoding="utf-8",
    )


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def test_campaign_probe_dry_run_writes_registry_and_summary(tmp_path: Path, monkeypatch) -> None:
    shops_path = tmp_path / "shops.yaml"
    fixture_path = tmp_path / "fixture.json"
    out_dir = tmp_path / "out"
    _write_shops(shops_path)

    fixture = {
        "samord": {
            "campaign_id_list": [
                {
                    "response": {
                        "campaign_list": [
                            {"campaign_id": 1001, "ad_type": "manual"},
                            {"campaign_id": 1002, "ad_type": "manual"},
                        ]
                    }
                }
            ],
            "setting_info": [
                {
                    "error": 0,
                    "message": "",
                    "response": {
                        "campaign_list": [
                            {
                                "campaign_id": 1001,
                                "common_info": {
                                    "ad_name": "Campaign A",
                                    "campaign_status": "ongoing",
                                    "campaign_budget": 120000,
                                    "campaign_duration": {
                                        "start_time": 1700000000,
                                        "end_time": 1700600000,
                                    },
                                    "item_id_list": [11, 22],
                                },
                            }
                        ]
                    },
                }
            ],
            "gms_campaign_perf": {
                "error": 0,
                "message": "",
                "response": {"records": [{"campaign_id": 1001}, {"campaign_id": 9999}]},
            },
        },
        "minmin": {
            "campaign_id_list": [
                {
                    "response": {
                        "campaign_list": [{"campaign_id": 2001, "ad_type": "manual"}]
                    }
                }
            ],
            "setting_info": [
                {
                    "error": 0,
                    "message": "",
                    "response": {
                        "campaign_list": [
                            {
                                "campaign_id": 2001,
                                "common_info": {
                                    "ad_name": "Campaign B",
                                    "campaign_status": "paused",
                                    "campaign_budget": 0,
                                    "campaign_duration": {
                                        "start_time": 1700100000,
                                        "end_time": 1700700000,
                                    },
                                    "item_id_list": [33],
                                },
                            }
                        ]
                    },
                }
            ],
            "gms_campaign_perf": {"error": 0, "message": "", "response": {"records": []}},
        },
    }
    fixture_path.write_text(json.dumps(fixture, ensure_ascii=False), encoding="utf-8")

    monkeypatch.setenv("SHOPS_CONFIG_PATH", str(shops_path))
    get_settings.cache_clear()

    ops_phase1_ads_campaign_probe(
        only_shops="samord,minmin",
        mode="dry-run",
        days=7,
        out=str(out_dir),
        redact=True,
        json_fixture=str(fixture_path),
        env_file=None,
    )

    registry_csv = out_dir / "normalized" / "campaign_registry.csv"
    summary_md = out_dir / "summary.md"
    assert registry_csv.exists()
    assert summary_md.exists()

    rows = _read_csv(registry_csv)
    assert len(rows) == 3  # includes one id_list_only row for campaign_id 1002
    for key in (
        "shop_label",
        "campaign_id",
        "ad_name",
        "daily_budget",
        "item_count",
        "source_endpoint",
    ):
        assert key in rows[0]

    summary_text = summary_md.read_text(encoding="utf-8")
    assert "VERDICT:" in summary_text
    assert "gms_parity=no" in summary_text


def test_campaign_probe_dry_run_missing_name_budget_still_generates_summary(
    tmp_path: Path, monkeypatch
) -> None:
    shops_path = tmp_path / "shops.yaml"
    fixture_path = tmp_path / "fixture_missing.json"
    out_dir = tmp_path / "out_missing"
    _write_shops(shops_path)

    fixture = {
        "samord": {
            "campaign_id_list": [{"response": {"campaign_list": [{"campaign_id": 3001}]}}],
            "setting_info": [
                {
                    "error": 0,
                    "message": "",
                    "response": {"campaign_list": [{"campaign_id": 3001, "common_info": {}}]},
                }
            ],
            "gms_campaign_perf": {"error": 0, "message": "", "response": {"records": []}},
        }
    }
    fixture_path.write_text(json.dumps(fixture, ensure_ascii=False), encoding="utf-8")

    monkeypatch.setenv("SHOPS_CONFIG_PATH", str(shops_path))
    get_settings.cache_clear()

    ops_phase1_ads_campaign_probe(
        only_shops="samord",
        mode="dry-run",
        days=7,
        out=str(out_dir),
        redact=True,
        json_fixture=str(fixture_path),
        env_file=None,
    )

    summary_text = (out_dir / "summary.md").read_text(encoding="utf-8")
    assert "VERDICT:" in summary_text
    assert "name=no" in summary_text
    assert "budget=no" in summary_text


def test_campaign_probe_live_invalid_token_preflight_sets_unknown(
    tmp_path: Path,
    monkeypatch,
) -> None:
    state_path = tmp_path / "rate_limit_state.json"
    call_log: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        call_log.append(str(request.url.path))
        if request.url.path == "/api/v2/ads/get_total_balance":
            return httpx.Response(
                403,
                json={
                    "error": "invalid_access_token",
                    "message": "invalid access token",
                    "request_id": "rid_token",
                },
            )
        return httpx.Response(200, json={"error": "", "message": "", "response": {}})

    mock_transport = httpx.MockTransport(handler)
    mock_client = ShopeeClient(
        partner_id=1,
        partner_key="k",
        host="https://example.com",
        transport=mock_transport,
    )

    monkeypatch.setattr(
        campaign_probe_mod,
        "_ensure_live_token",
        lambda settings, shop_cfg: ("access_token_x", int(shop_cfg.shopee_shop_id)),
    )
    monkeypatch.setattr(campaign_probe_mod, "_build_client", lambda settings: mock_client)
    monkeypatch.setattr(campaign_probe_mod, "_default_rate_limit_state_path", lambda: state_path)

    target_shops = [
        SimpleNamespace(shop_key="samord", label="SAMORD", enabled=True, shopee_shop_id=497412318)
    ]
    settings = SimpleNamespace(
        shopee_partner_id=1,
        shopee_partner_key="k",
        shopee_api_host="https://example.com",
    )

    result = run_campaign_probe(
        settings=settings,
        target_shops=target_shops,
        mode="live",
        days=1,
        out_dir=tmp_path / "out_live_invalid",
        redact=True,
        fixture_payload=None,
        max_requests_per_shop=5,
        sync_db=False,
    )
    shop = result["shop_results"][0]
    assert shop.preflight_ok is False
    assert shop.preflight_reason == "token_invalid"
    assert shop.meta_probe_ok is False
    assert shop.id_list_count == 0
    assert call_log == ["/api/v2/ads/get_total_balance"]

    summary_text = (tmp_path / "out_live_invalid" / "summary.md").read_text(encoding="utf-8")
    assert "name=unknown" in summary_text
    assert "budget=unknown" in summary_text
    assert "preflight_ok: 0" in summary_text

    mock_client.close()


def test_call_live_429_retry_after_sets_cooldown_and_next_call_skips(
    tmp_path: Path, monkeypatch
) -> None:
    state_path = tmp_path / "rate_limit_state.json"
    call_count = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        call_count["n"] += 1
        return httpx.Response(
            429,
            headers={"Retry-After": "120"},
            json={"error": "error_rate_limit", "message": "Too many requests"},
        )

    client = ShopeeClient(
        partner_id=1,
        partner_key="k",
        host="https://example.com",
        transport=httpx.MockTransport(handler),
    )
    monkeypatch.setattr(campaign_probe_mod, "_default_rate_limit_state_path", lambda: state_path)

    payload_1, _, trace_1 = _call_live(
        client=client,
        shop_key="samord",
        path="/api/v2/ads/get_gms_campaign_performance",
        shop_id=497412318,
        access_token="token",
        params={"start_date": "2026-03-04", "end_date": "2026-03-04"},
    )
    assert payload_1 is not None
    assert trace_1.rate_limited is True
    assert trace_1.retry_after_sec == 120
    assert call_count["n"] == 1

    payload_2, _, trace_2 = _call_live(
        client=client,
        shop_key="samord",
        path="/api/v2/ads/get_gms_campaign_performance",
        shop_id=497412318,
        access_token="token",
        params={"start_date": "2026-03-04", "end_date": "2026-03-04"},
    )
    assert payload_2 is not None
    assert payload_2.get("error") == "local_rate_limited"
    assert trace_2.skipped_by_cooldown is True
    assert trace_2.rate_limited is True
    assert call_count["n"] == 1

    state = json.loads(state_path.read_text(encoding="utf-8"))
    assert "samord" in state.get("shops", {})

    client.close()


def test_call_live_ads_rate_limit_total_api_marks_rate_limited_even_without_429(
    tmp_path: Path, monkeypatch
) -> None:
    state_path = tmp_path / "rate_limit_state_403.json"

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            403,
            json={
                "error": "ads_rate_limit_total_api",
                "message": "ads_rate_limit_total_api",
                "request_id": "rid_rl",
            },
        )

    client = ShopeeClient(
        partner_id=1,
        partner_key="k",
        host="https://example.com",
        transport=httpx.MockTransport(handler),
    )
    monkeypatch.setattr(campaign_probe_mod, "_default_rate_limit_state_path", lambda: state_path)

    _, _, trace = _call_live(
        client=client,
        shop_key="minmin",
        path="/api/v2/ads/get_gms_campaign_performance",
        shop_id=567655304,
        access_token="token",
        params={"start_date": "2026-03-04", "end_date": "2026-03-04"},
    )
    assert trace.http_status == 403
    assert trace.rate_limited is True
    assert trace.api_error == "ads_rate_limit_total_api"

    state = json.loads(state_path.read_text(encoding="utf-8"))
    assert "minmin" in state.get("shops", {})
    assert str(state["shops"]["minmin"].get("cooldown_until_utc") or "").strip()

    client.close()
