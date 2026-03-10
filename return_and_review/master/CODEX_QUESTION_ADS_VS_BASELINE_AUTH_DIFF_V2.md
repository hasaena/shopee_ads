# CODEX / 다른 AI용 정밀 조사 문서 (개정판)
## 주제
광고 프로젝트와 공용 디렉토리를 기준으로 Shopee 인증/토큰/샵 바인딩 차이를 끝까지 좁히기

## 이 문서의 목적
현재 상황은 다음과 같다.

- Shopee 주문/재고/토큰 관리는 Google Apps Script(GAS)에서 담당
- 광고 기능은 GAS -> 서버 token push/import -> 서버에서 Shopee API 사용
- returns/reviews live probe는 403
- baseline 일반 shop API probe도 403
- 광고 기능은 실제로 어딘가에서 성공하고 있다고 들음
- 프로젝트는 광고 프로젝트와 returns/reviews 작업물이 같은 큰 상위 디렉토리 안에 폴더만 나눠서 존재할 가능성이 높음

따라서 지금 필요한 것은 새 코드 작성이 아니라:

**이미 성공 중인 광고 기능의 실제 Shopee 호출 경로를 실증하고,  
그 경로와 returns/reviews 실패 경로의 차이를 증거 기반으로 비교하는 것**이다.

---

# 절대 원칙
1. 추측 금지. 반드시 파일/코드/로그/문서/설정/샘플/DB 근거를 제시할 것
2. access token 원문 절대 출력 금지
3. refresh/token ownership 구조를 개선 제안하지 말고, 현재 구조 해부를 우선할 것
4. GAS 수정 제안 금지
5. 서버 refresh 도입 제안 금지
6. 광고 기능이 실제로 성공하는 “구체적 API 1개 이상”을 반드시 찾아낼 것
7. 상위 공용 디렉토리 전체를 적극적으로 탐색할 것

---

# 최종적으로 밝혀야 하는 핵심 질문
아래 6가지를 최종적으로 밝혀야 한다.

1. **광고 기능은 실제로 어떤 Shopee API를 어떤 자격증명/토큰/샵으로 성공시키는가?**
2. **광고 기능과 returns/reviews는 정말 같은 token row / 같은 shop_id / 같은 app/partner credential을 보는가?**
3. **광고 기능과 returns/reviews는 sign 생성 방식과 host/path 구성이 정말 같은가?**
4. **GAS -> 서버 token import 계약이 실제로 무엇이며, 최근 성공 import 흔적이 무엇인가?**
5. **403의 가장 가능성 높은 원인은 token source 문제가 아니라 더 상위(credential/app/shop binding/host/path/sign) 문제인가?**
6. **사용자에게 정말 필요한 확인 질문은 최소 몇 개이며 무엇인가?**

---

# 조사 범위
상위 프로젝트 디렉토리에서 아래 위치를 반드시 우선 조사하라.
실제 이름이 약간 달라도 유사 폴더를 포함한다.

- `config/`
- `docs/`
- `scripts/`
- `logs/`
- `artifacts/`
- `samples/`
- `deploy/`
- `ops/`
- `apps_script/`
- `gas/`
- `ads/`
- `shared/`
- `common/`
- `reports/`
- `results/`
- `legacy/`
- `tmp/`
- `debug/`

특히 아래 키워드를 기준으로 폭넓게 찾아라.

- `shopee`
- `ads`
- `token import`
- `phase1/token/import`
- `shop_id`
- `partner_id`
- `partner_key`
- `sign`
- `access_only`
- `token_mode`
- `get_shop_info`
- `get_profile`
- `balance`
- `report`
- `campaign`
- `keyword`
- `403`
- `forbidden`
- `auth`
- `preflight`

---

# 반드시 수행할 조사 항목

## A. 광고 기능의 “실제 성공 API” 실증
“광고가 된다”는 설명만으로는 부족하다.  
반드시 **현재 코드/로그/문서 기준으로 실제 성공 Shopee Ads API 한 개 이상**을 특정하라.

### 반드시 밝혀야 할 것
- feature 이름 (예: ads balance, ads report, keyword report, campaign list 등)
- 실제 endpoint/path
- 실제 host
- 실제 사용 shop key
- 실제 사용 shop_id
- 실제 token source
- 실제 code path (파일 + 함수)
- 실제 성공 근거
  - 코드 호출 경로
  - 마지막 성공 로그
  - saved artifact
  - report output
  - cron/scheduler registration
  중 최소 1개 이상

### 출력 형식
- feature:
- host:
- path:
- shop_key:
- shop_id:
- token_source:
- code_path:
- success_evidence:
- evidence_file:

### 중요
성공 API는 최소 1개, 가능하면 2개 이상 찾아라.

---

## B. 광고 기능과 returns/reviews의 token lineage 비교
“같은 토큰을 쓴다”는 말이 아니라, 실제로 **같은 token row / same fingerprint / same source / same expiry**인지 확인하라.

### 확인할 것
- 광고 기능이 token row를 찾는 기준
- returns/reviews가 token row를 찾는 기준
- 둘이 같은 DB table을 보는지
- 둘이 같은 shop key / shop_id를 쓰는지
- 둘이 같은 access token fingerprint를 보는지
- 둘이 같은 expires_at / updated_at 을 보는지
- last_ingested_at이 같은 흐름에서 생긴 것인지

### 출력 형식
- ads_token_table:
- rr_token_table:
- ads_lookup_key:
- rr_lookup_key:
- ads_shop_key:
- rr_shop_key:
- ads_shop_id:
- rr_shop_id:
- ads_token_fingerprint:
- rr_token_fingerprint:
- same_or_diff:
- evidence_files:

### 추가 요구
가능하면 token row를 설명하는 **sanitized 표**를 만들어라.
(원문 token 금지, fingerprint/hash/마스킹만 허용)

---

## C. app / partner credential lineage 비교
403의 원인이 token 자체가 아니라 **다른 partner/app credential 사용**일 수 있다.
이 부분을 꼭 확인하라.

### 반드시 확인할 것
- 광고 기능이 참조하는 partner_id / partner_key / client config 소스
- returns/reviews가 참조하는 partner_id / partner_key / client config 소스
- 두 기능이 같은 `.env`, same yaml, same secret source를 쓰는지
- ads 기능만 별도 credential 파일을 쓰는지
- stage/prod host 차이가 있는지
- 다른 app id / 다른 partner account 가능성이 있는지

### 출력 형식
- ads_partner_id_source:
- rr_partner_id_source:
- ads_partner_key_source:
- rr_partner_key_source:
- same_or_diff:
- config_files:
- comment:

---

## D. signing / request assembly 1:1 diff
이 항목은 반드시 표로 비교하라.

### 비교 항목
- host
- path
- method
- partner_id source
- partner_key source
- shop_id source
- access_token source
- timestamp source
- sign base string 생성 함수
- HMAC 함수
- request query/body 조립 함수
- preflight 적용 위치

### 출력 표 형식
| 항목 | ads 경로 | returns/reviews 경로 | 동일/상이 | 근거 파일 |
|---|---|---|---|---|

### 주의
“같아 보인다” 금지.  
반드시 파일/함수 기준으로 적어라.

---

## E. host / endpoint family 차이 확인
광고 기능과 일반 Shopee shop/order/product API는 host/path family가 다를 수 있다.
이걸 명확히 밝혀라.

### 확인할 것
- ads 기능이 어떤 base host를 쓰는지
- baseline probe가 어떤 base host를 쓰는지
- 두 기능이 같은 domain이지만 path family만 다른지
- 아예 host 자체가 다른지
- ads 전용 gateway / ads manager wrapper가 있는지
- partner.shopeemobile.com 계열 외 다른 경로가 있는지

### 출력 형식
- ads_base_host:
- baseline_base_host:
- same_or_diff:
- ads_path_family:
- baseline_path_family:
- comment:
- evidence_files:

---

## F. GAS -> 서버 token import 계약 실제 구조
이건 문서/코드/샘플/로그에서 최대한 실체를 찾아라.

### 반드시 확인할 것
- import endpoint 경로
- method
- payload top-level key
- shops 배열 구조
- access_only 구조 근거
- refresh_token 전달 여부
- expires_at 포함 여부
- shop_key / shop_id / source 포함 여부
- import success / noop / fail 응답 구조
- 최근 import 로그 흔적

### 출력 형식
- endpoint:
- method:
- payload_schema_sanitized:
- includes_refresh_token: yes/no/unknown
- includes_shop_id: yes/no/unknown
- includes_expires_at: yes/no/unknown
- response_shape:
- evidence_files:

### 중요
샘플 payload를 꼭 sanitizing 해서 넣어라.

---

## G. “최근 실제 성공 import”와 “최근 실제 광고 성공 실행”의 시간축 비교
시간축이 중요하다.

### 확인할 것
- 최근 token import 성공 시각
- 최근 ads job 성공 시각
- 같은 shop에 대한 마지막 성공 시각
- ttl 관점에서 그 토큰이 당시 유효했는지
- returns/reviews probe 실행 시점과 비교했을 때 시간 간격

### 출력 형식
- last_token_import_at:
- last_ads_success_at:
- last_rr_probe_at:
- same_shop: yes/no/unknown
- ttl_consistency_comment:
- evidence_files:

---

## H. 공용 디렉토리 안 문서/로그/결과 파일 스윕
아래 유형의 파일을 적극적으로 찾아라.

### 찾을 문서 유형
- 이전 RESULT md
- auth/token 설계 문서
- ads 배포 문서
- 운영 runbook
- import payload 샘플
- debug log 저장본
- saved API response
- cron/systemd/pm2 설정
- env 설명 파일
- README 내부 운영 메모

### 각 파일마다 정리할 것
- 파일 경로
- 왜 지금 문제에 도움이 되는지
- 핵심 내용 2~3줄

### 중요
“도움 안 되는 파일”은 굳이 나열하지 말고, 정말 유의미한 것만 뽑아라.

---

## I. 403 원인 가설의 우선순위화
최종적으로 아래 후보를 **우선순위 순서대로** 정렬하라.

### 후보 예시
- 서로 다른 app/partner credential 사용
- ads와 baseline이 서로 다른 host/path family 사용
- shop_id 바인딩 불일치
- token row mismatch
- sign base string 미세 차이
- stale token / import timing 문제
- ads는 seller shop credential이 아니라 별도 credential 사용
- returns/reviews가 실제 같은 환경을 못 보고 있음

### 출력 형식
1. 가설
2. 왜 가장 유력한지
3. 근거 파일
4. 반증 여부
5. 다음에 확인할 최소 행동

---

## J. 사용자에게 꼭 물어봐야 할 최소 질문
질문은 많이 만들지 말고, 정말 필요한 것만 3~7개로 제한하라.

### 좋은 질문의 기준
- 코드/로그/문서만으로는 확정할 수 없는 것
- 사용자만 바로 답할 수 있는 것
- 답을 들으면 원인 후보를 크게 줄일 수 있는 것

### 나쁜 질문의 기준
- 이미 디렉토리 안에서 찾을 수 있는 것
- 로그/코드 보면 되는 것
- 추상적이고 넓은 질문

---

# 추가로 반드시 비교해야 하는 것
아래 항목은 누락되기 쉬우니 꼭 확인하라.

1. **광고 기능이 정말 same shop_key=samord 기준으로 도는지**
2. **광고 기능이 same DB / same env / same config profile을 쓰는지**
3. **광고 기능이 실제 Shopee seller API가 아니라 ads 전용 wrapper/SDK 경로를 타는지**
4. **returns/reviews baseline probe가 일반 host를 치는데, ads는 전혀 다른 host/path family를 치는지**
5. **ads 성공 로그와 returns/reviews 실패 로그의 correlation 가능한 timestamp가 있는지**
6. **상위 디렉토리에 별도 `.env`, `.env.prod`, `shops.yaml`, `ads.yaml`, `credentials.json` 같은 분기 설정이 있는지**
7. **import payload에서 shop_id와 shop_key의 매핑 근거가 어디서 오는지**
8. **ads는 최근 성공했지만 해당 shop token은 지금 DB에 없는/다른 경우가 아닌지**
9. **ads 기능은 one shop, returns/reviews는 다른 shop에 대해 probe 중인 것은 아닌지**
10. **returns/reviews probe가 baseline용 सामान्य shop API를 치는데, ads 성공은 seller API 성격이 다른 것인지**

---

# 최종 결과 문서 형식
파일명:
`RESULT-004A_ADS_VS_BASELINE_AUTH_DIFF_DISCOVERY.md`

## 필수 섹션
1. 요약
2. 광고 성공 API 실체
3. 광고 vs returns/reviews token lineage 비교
4. 광고 vs returns/reviews credential lineage 비교
5. 광고 vs returns/reviews signing/request assembly diff
6. host/path family 비교
7. GAS token import payload 구조 및 최근 import 근거
8. 최근 token import vs ads success 시간축 비교
9. 공용 디렉토리에서 찾은 유의미한 문서/로그/결과 파일
10. 403 원인 가설 우선순위
11. 사용자에게 꼭 물어봐야 할 최소 질문
12. 다음 TASK에 반영할 결론

---

# 최종 산출물 요구
문서만 주지 말고, 가능하면 아래 artifact도 함께 남겨라.

1. `artifacts/auth_diff_matrix.md`
2. `artifacts/token_lineage_sanitized.json`
3. `artifacts/gas_import_payload_sanitized.json`
4. `artifacts/ads_success_evidence_index.md`

민감정보는 반드시 제거할 것.

---

# 가장 중요한 한 줄
이번 조사 목적은 새 기능 구현이 아니다.

**이미 돌아가고 있는 광고 기능이 정확히 어떤 Shopee 인증/토큰/샵/호스트/사인 흐름으로 성공하는지 실증하고,  
그 흐름과 returns/reviews 실패 경로의 차이를 증거 기반으로 비교하는 것**이다.
