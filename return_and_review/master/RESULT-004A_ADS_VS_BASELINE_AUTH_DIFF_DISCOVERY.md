# RESULT-004A_ADS_VS_BASELINE_AUTH_DIFF_DISCOVERY

## 1. 요약
- 목적대로 “새 코드 구현”이 아니라 **기존 광고 성공 경로 vs returns/reviews 403 경로**를 파일/코드/아티팩트 기준으로 비교했다.
- 결론 요약:
  1) 광고 성공 API는 실증 가능하다 (`/api/v2/ads/get_total_balance`, `/api/v2/ads/check_create_gms_product_campaign_eligibility`, `/api/v2/ads/get_all_cpc_ads_daily_performance`).
  2) ads와 returns/reviews는 **코드 상 partner credential, signing 함수, host 소스는 동일**하다.
  3) 하지만 **실행 컨텍스트(DB/토큰 라인리지)가 다를 정황이 강함**:
     - returns/reviews 진단 토큰 sha8=`9bdd6e28` (`collaboration/local.db`와 일치)
     - ads 성공 artifact의 토큰 sha8=`40989394` 등으로 불일치
  4) returns/reviews는 baseline(`/api/v2/shop/get_shop_info`, `/api/v2/shop/get_profile`)까지 403이라, 단순 endpoint 권한 문제만으로 설명되지 않는다.

---

## 2. 광고 성공 API 실체

### A) feature: ads balance
- host: `https://partner.shopeemobile.com`
- path: `/api/v2/ads/get_total_balance`
- shop_key: `samord`, `minmin`
- shop_id: `497412318`, `567655304`
- token_source: DB (`sweep.md`에 `token_source=db`)
- code_path:
  - `src/dotori_shopee_automation/ads/campaign_probe.py:850`
  - `src/dotori_shopee_automation/ads/campaign_probe.py:838`
- success_evidence:
  - checks json에서 `ok=1`, request_id 존재, `response.total_balance` 존재
- evidence_file:
  - `collaboration/artifacts/server_ads_gms_guidance_20260305/samord_checks.json`
  - `collaboration/artifacts/server_ads_gms_guidance_20260305/minmin_checks.json`

### B) feature: gms eligibility
- host: `https://partner.shopeemobile.com`
- path: `/api/v2/ads/check_create_gms_product_campaign_eligibility`
- shop_key: `samord`, `minmin`
- shop_id: `497412318`, `567655304`
- token_source: DB
- code_path:
  - endpoint 선택/검증 흐름: `src/dotori_shopee_automation/ads/campaign_probe.py`
- success_evidence:
  - checks json에서 `ok=1`, `response.is_eligible=true`
- evidence_file:
  - `collaboration/artifacts/server_ads_gms_guidance_20260305/samord_checks.json`
  - `collaboration/artifacts/server_ads_gms_guidance_20260305/minmin_checks.json`

### C) feature: ads daily performance
- host: `https://partner.shopeemobile.com`
- path: `/api/v2/ads/get_all_cpc_ads_daily_performance`
- shop_key: `samord`, `minmin`
- shop_id: `497412318`, `567655304`
- token_source: DB
- code_path:
  - daily truth 수집/저장 흐름: `src/dotori_shopee_automation/cli.py` (ads daily truth 계열)
- success_evidence:
  - payload `__meta.ok=true`, request_id 존재, response row 존재
- evidence_file:
  - `collaboration/artifacts/server_ads_gms_guidance_20260305/samord_ads_daily_truth_ads_daily.json`
  - `collaboration/artifacts/server_ads_gms_guidance_20260305/minmin_ads_daily_truth_ads_daily.json`

---

## 3. 광고 vs returns/reviews token lineage 비교

- ads_token_table: `shopee_tokens`
- rr_token_table: `shopee_tokens`
- ads_lookup_key: `shop_key` (`get_token(session, shop_cfg.shop_key)`)
- rr_lookup_key: `shop_key` (`get_token(session, shop_cfg.shop_key)`)
- ads_shop_key: `samord|minmin`
- rr_shop_key: `samord` (진단 실행 대상)
- ads_shop_id: 497412318 / 567655304 (artifact 기준)
- rr_shop_id: 497412318 (`live_probe_diagnosis_samord.json`)
- ads_token_fingerprint:
  - ads 성공 artifact: `40989394` (minmin sweep)
  - 과거 live DB: samord `92d21936`, minmin `0a7aba70` (`collaboration/phase1_live.db`)
- rr_token_fingerprint:
  - `9bdd6e28` (`live_probe_diagnosis_samord.json`)
  - 현재 runtime DB(`collaboration/local.db`)의 samord/minmin과 일치
- same_or_diff: **diff 가능성 높음 (실행 컨텍스트/DB가 다를 정황)**
- evidence_files:
  - `src/dotori_shopee_automation/ads/campaign_probe.py:809`
  - `src/dotori_shopee_automation/returns_reviews/service.py:207`
  - `return_and_review/collaboration/artifacts/live_probe_diagnosis_samord.json`
  - `collaboration/artifacts/server_ads_gms_guidance_20260305/sweep.md`
  - `collaboration/local.db`
  - `collaboration/phase1_live.db`

(정리 JSON: `return_and_review/master/artifacts/token_lineage_sanitized.json`)

---

## 4. 광고 vs returns/reviews credential lineage 비교

- ads_partner_id_source: `get_settings().shopee_partner_id`
- rr_partner_id_source: `get_settings().shopee_partner_id`
- ads_partner_key_source: `get_settings().shopee_partner_key`
- rr_partner_key_source: `get_settings().shopee_partner_key`
- same_or_diff: **same (코드 경로 기준)**
- config_files:
  - `src/dotori_shopee_automation/config.py`
  - `.env` (pydantic settings source)
- comment:
  - 코드 상 credential 소스는 동일하다.
  - 다만 runtime DB URL이 `sqlite:///.../collaboration/local.db`로 해석되고 있어, ads 성공 시점 artifact와 동일 런타임인지 별도 확인이 필요하다.

---

## 5. 광고 vs returns/reviews signing/request assembly diff

- 상세 표는 `return_and_review/master/artifacts/auth_diff_matrix.md` 참조.
- 요약:
  - `build_sign_base`, `sign_hmac_sha256_hex`, `ShopeeClient.request`는 공통 사용(동일)
  - 차이점은 **호출 path family**와 **preflight 구조**, 그리고 ads 경로의 refresh 가능 코드 존재 여부

---

## 6. host/path family 비교

- ads_base_host: `https://partner.shopeemobile.com`
- baseline_base_host: `https://partner.shopeemobile.com`
- same_or_diff: host는 same
- ads_path_family: `/api/v2/ads/*`
- baseline_path_family: `/api/v2/shop/*`, `/api/v2/returns/*`, `/api/v2/product/*`
- comment:
  - host는 같고 path family가 다르다.
  - 즉 “도메인 분기”보다 “API family/권한/바인딩” 축에서 원인을 봐야 한다.
- evidence_files:
  - `src/dotori_shopee_automation/config.py:49`
  - `src/dotori_shopee_automation/ads/campaign_probe.py:850`
  - `src/dotori_shopee_automation/returns_reviews/service.py:275`
  - `src/dotori_shopee_automation/returns_reviews/shopee_returns_api.py:111`
  - `src/dotori_shopee_automation/returns_reviews/shopee_reviews_api.py:104`

---

## 7. GAS token import payload 구조 및 최근 import 근거

- endpoint: `/ops/phase1/token/import`
- method: `POST`
- payload_schema_sanitized:
  - top-level: `version`, `token_mode`, `source`, `pushed_at`, `shops`
  - `shops.<shop_id>.access_token` 필수
  - expiry는 `expire_timestamp`/`access_expire_timestamp`/`expires_in` 등 다중 키 허용
- includes_refresh_token: **yes (payload는 허용), but 저장은 no (discard)**
- includes_shop_id: yes (map key 혹은 payload.shop_id)
- includes_expires_at: yes (optional)
- response_shape:
  - `ok`, `request_id`, `imported`, `noop`, `shops`, `token_sha8`, `discarded_refresh_tokens`, `ignored_shop_ids` 등
- evidence_files:
  - `src/dotori_shopee_automation/webapp.py:285`
  - `src/dotori_shopee_automation/webapp.py:1566`
  - `src/dotori_shopee_automation/webapp.py:1689`
  - `collaboration/tmp/task_123/status_sample_import.json`
  - `collaboration/tmp/task_123/status_sample_status.json`

(샘플 정리 JSON: `return_and_review/master/artifacts/gas_import_payload_sanitized.json`)

---

## 8. 최근 token import vs ads success 시간축 비교

- last_token_import_at:
  - evidence 기준 최신 샘플: `2026-02-28T09:17:25.295045Z`
  - source: `collaboration/tmp/task_123/status_sample_status.json`
- last_ads_success_at:
  - checks generated_at: `2026-03-05T09:43:38Z` (samord/minmin)
  - source: `collaboration/artifacts/server_ads_gms_guidance_20260305/*_checks.json`
- last_rr_probe_at:
  - artifact mtime: `2026-03-09T09:34:03Z`
  - source: `return_and_review/collaboration/artifacts/live_probe_diagnosis_samord.json`
- same_shop: yes (samord/minmin)
- ttl_consistency_comment:
  - rr probe는 preflight상 TTL 충분(`preflight_ok`)인데 baseline부터 403.
  - 따라서 단순 “만료 토큰” 단일 원인보다는, 실행 컨텍스트 불일치(토큰 라인리지/DB/실행 프로필) 또는 상위 바인딩 문제가 더 유력.
- evidence_files:
  - `collaboration/tmp/task_123/status_sample_status.json`
  - `collaboration/artifacts/server_ads_gms_guidance_20260305/samord_checks.json`
  - `collaboration/artifacts/server_ads_gms_guidance_20260305/minmin_checks.json`
  - `return_and_review/collaboration/artifacts/live_probe_diagnosis_samord.json`

---

## 9. 공용 디렉토리에서 찾은 유의미한 문서/로그/결과 파일

1) `collaboration/artifacts/server_ads_gms_guidance_20260305/sweep.md`
- 왜 유의미한가: ads endpoint sweep 결과를 한 파일에서 확인 가능
- 핵심: `token_source=db`, baseline 200, ads endpoint별 ok/fail, request_id 포함

2) `collaboration/artifacts/server_ads_gms_guidance_20260305/samord_checks.json`
- 왜 유의미한가: samord 기준 ads 성공 endpoint의 구조화 증거
- 핵심: total_balance/gms_eligibility `ok=1`, request_id 기록

3) `collaboration/artifacts/server_ads_gms_guidance_20260305/minmin_checks.json`
- 왜 유의미한가: minmin 기준 동일 증거
- 핵심: total_balance/gms_eligibility `ok=1`, request_id 기록

4) `return_and_review/collaboration/artifacts/live_probe_diagnosis_samord.json`
- 왜 유의미한가: returns/reviews 실패 원인을 한 번에 묶은 진단 산출물
- 핵심: token preflight ok인데 baseline/returns/reviews 모두 403, safe fingerprint 포함

5) `collaboration/tmp/task_123/status_sample_import.json`
- 왜 유의미한가: `/ops/phase1/token/import` 성공 응답 샘플
- 핵심: `token_mode=access_only`, imported/noop, token_sha8, auto_resume 구조

6) `collaboration/tmp/task_123/status_sample_status.json`
- 왜 유의미한가: import 이후 상태 API 샘플
- 핵심: per-shop token_source/token_mode/token_import_last_at 확인 가능

7) `src/dotori_shopee_automation/webapp.py`
- 왜 유의미한가: import 계약과 auth 검증의 실제 구현
- 핵심: `@app.post('/ops/phase1/token/import')`, refresh discard, event log 기록

8) `src/dotori_shopee_automation/ads/campaign_probe.py`
- 왜 유의미한가: ads 성공 경로의 실제 call path
- 핵심: preflight endpoint, sign/query 조립, live token 사용 경로

9) `src/dotori_shopee_automation/returns_reviews/service.py`
- 왜 유의미한가: rr baseline/probe 경로 및 token gate 동작
- 핵심: baseline path, get_token usage, preflight, 403 분류

10) `config/shops.yaml`
- 왜 유의미한가: shop_key 기본 매핑 소스
- 핵심: 파일 기본값 shop_id(111111/222222)와 env override 구조가 있어 런타임 불일치 검증 포인트

---

## 10. 403 원인 가설 우선순위

1) 가설: **ads 성공 실행과 rr probe 실행이 서로 다른 DB/토큰 라인리지를 보고 있다**
- 유력 이유: rr token sha8=`9bdd6e28` vs ads artifact token sha8=`40989394` 등 불일치
- 근거 파일: `live_probe_diagnosis_samord.json`, `sweep.md`, `collaboration/local.db`, `collaboration/phase1_live.db`
- 반증 여부: 아직 없음
- 다음 최소 행동: ads 성공을 낸 동일 프로세스에서 `database_url`, token sha8, request_id를 동시에 로그로 채집

2) 가설: **shop binding/app 승인 상태가 rr path family에서 거부되고 있다**
- 유력 이유: baseline `/shop/*` 자체가 403
- 근거 파일: `live_probe_diagnosis_samord.json`
- 반증 여부: ads family 성공이 존재하므로 “전체 계정 차단”은 아님
- 다음 최소 행동: 동일 token sha8로 `/shop/get_shop_info`와 `/ads/get_total_balance`를 같은 실행에서 back-to-back 호출 비교

3) 가설: **shop_id 해석 소스 차이(env override vs config)로 잘못된 shop binding이 섞인다**
- 유력 이유: `shops.yaml` 기본값이 실샵 id와 다르고 env override에 의존
- 근거 파일: `config/shops.yaml`, `cli.py:_resolve_shop_id`, `returns_reviews/service.py:_resolve_shop_id`
- 반증 여부: rr 진단 산출물에는 shop_id=497412318이 찍혀 부분 반증
- 다음 최소 행동: 실행 시점마다 shop_key->shop_id 해석값을 명시 로그화

4) 가설: **signing 구현 차이**
- 유력 이유: 일반적으로 403 원인 후보
- 근거 파일: `shopee/signing.py`, `shopee/client.py`, `campaign_probe.py`, `returns_reviews/service.py`
- 반증 여부: 함수/조립은 실질 동일로 강한 반증
- 다음 최소 행동: 우선순위 낮춤

5) 가설: **stale token/import timing 문제**
- 유력 이유: 운영상 자주 등장하는 원인
- 근거 파일: `status_sample_status.json`, `live_probe_diagnosis_samord.json`
- 반증 여부: rr preflight가 OK라 단독 원인으로는 약함
- 다음 최소 행동: import 직후 즉시 rr baseline 재검증

---

## 11. 사용자에게 꼭 물어봐야 할 최소 질문

1) ads 성공을 실제 발생시킨 실행 컨텍스트는 무엇인가요?
- (예: systemd 서비스명/호스트, 사용 DB URL)

2) rr probe를 돌린 컨텍스트와 ads 성공 컨텍스트가 100% 동일한 프로세스/환경인가요?
- (같은 `.env`, 같은 `database_url`, 같은 코드 revision)

3) ads 성공 직후의 request_id 1개와, 같은 시점 rr baseline 403 request_id 1개를 줄 수 있나요?
- 두 요청의 token sha8/shop_id를 같은 로그에서 대조하려고 합니다.

4) `samord/minmin` 실샵 ID는 env에서만 관리합니까, 아니면 `shops.yaml`도 운영값으로 유지합니까?
- 지금 구조는 두 소스가 공존해서 런타임 혼선 가능성이 있습니다.

5) rr 경로가 반드시 ads와 “동일 app/partner credential”을 써야 하나요?
- 만약 의도적으로 분리된 credential이라면 403 원인 축이 즉시 좁혀집니다.

---

## 12. 다음 TASK에 반영할 결론

1) **관측 통일 태스크**: 단일 실행에서 ads success API + baseline API를 연속 호출하고, 동일 로그에 `shop_key/shop_id/token_sha8/request_id/database_url`를 남기는 진단 명령을 만든다.
2) **실행 프로필 고정 태스크**: rr probe와 ads job 모두 동일 `.env`/동일 DB로 강제되도록 실행 래퍼를 분리한다.
3) **shop_id 해석 고정 태스크**: shop_id source 우선순위를 한 곳으로 통일하고, 시작 시 해석값을 고정 로그화한다.
4) **token import 추적 태스크**: `/ops/phase1/token/import` 이벤트와 실제 API 호출을 request_id/sha8 기준으로 연결하는 운영 리포트를 추가한다.

---

## 추가 산출물
- `return_and_review/master/artifacts/auth_diff_matrix.md`
- `return_and_review/master/artifacts/token_lineage_sanitized.json`
- `return_and_review/master/artifacts/gas_import_payload_sanitized.json`
- `return_and_review/master/artifacts/ads_success_evidence_index.md`
