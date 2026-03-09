# CODEX_QUESTION_TOKEN_FLOW_ALIGNMENT

## 목적
반품/리뷰 시스템(Codex B)에서 Shopee 토큰을 사용할 때, 현재 광고 시스템(Codex A)과 충돌 없이 동일한 운영 원칙으로 맞추기 위한 정렬 문서.

기준 시각: 2026-03-09 (ICT, UTC+7)

---

## 1) 현재 운영 사실(광고 시스템 기준)

- 토큰 마스터는 **Apps Script**.
- 서버는 `/ops/phase1/token/import` 로 **access token만 수신** (`token_mode=access_only`).
- 서버 DB에는 refresh token을 저장/사용하지 않음 (`has_refresh_token=0` 상태).
- 스케줄러/리포트/알림은 DB 토큰을 읽기만 하고, 토큰 갱신은 하지 않음(패시브 소비자 구조).
- `imported=0, noop=2` 는 실패가 아니라 “기존 DB와 동일 토큰이라 변경 없음” 의미.

---

## 2) 충돌 방지 원칙 (반품/리뷰 시스템 필수)

### A. 단일 토큰 마스터 원칙
- 토큰 회전(refresh)은 **Apps Script에서만** 수행.
- 서버/다른 프로젝트에서 refresh 호출 금지.
- 여러 시스템이 필요해도 “한 곳에서 갱신 -> 각 시스템으로 push/import” 구조 유지.

### B. 소비자 원칙
- 각 시스템은 DB에 저장된 access token을 읽어 API 호출.
- TTL 부족 시 API 강행하지 말고 preflight fail 처리 + 알림.

### C. 분리 원칙
- 광고 시스템 DB와 반품/리뷰 시스템 DB는 분리 가능.
- 분리 시에도 push payload 계약(JSON 스키마)은 동일하게 유지.

---

## 3) 토큰 push/import 계약 (공통)

Apps Script -> 서버 `POST /ops/phase1/token/import`

필수 개념:
- `source`: 예) `appsscript_push_access_only`
- `token_mode`: `access_only`
- `shops`: shop_id별 access token + 만료시각

서버 응답 해석:
- `ok=true`: 수신/검증 성공
- `imported>0`: DB 반영 발생
- `noop>0`: 기존과 동일해서 미반영(정상)
- `ok=false` 또는 HTTP 5xx: import 실패(알림 대상)

---

## 4) 현재 점검 결과 (운영 안정성)

점검 항목:
- `/ops/phase1/status` 응답 정상 (`ok=true`)
- DB 최신 ingest 날짜: 두 샵 모두 `2026-03-09`
- 토큰 상태: 두 샵 모두 `gate_state=ok`, `token_mode=access_only`
- 리포트 파일 존재:
  - final: 2026-03-05~2026-03-08 연속 존재 확인
  - midday/weekly 최신 파일 존재 확인

주의:
- 상태 API의 “latest final”은 파일 **mtime 기준**이라, 과거 날짜 파일을 재생성하면 최신 포인터가 해당 날짜로 보일 수 있음.
- “누적 구조 이상”이 아니라 “latest 선택 로직(정렬 기준)” 이슈.

---

## 5) 반품/리뷰 시스템 구현 권장안

### 권장안 (가장 안전)
1. Apps Script 토큰 갱신 + push는 기존처럼 유지.
2. 반품/리뷰 시스템에도 동일한 token import endpoint 구현(또는 공용 토큰 서비스 경유).
3. 반품/리뷰 시스템은 access token TTL preflight 후 API 호출.
4. preflight fail 시 작업 스킵 + 경고 카드 알림.

### 금지안
- 반품/리뷰 시스템이 refresh token으로 독자 갱신
- 광고 시스템/반품 시스템이 서로 다른 토큰 회전 정책 사용
- 한쪽에서 refresh 후 다른쪽에서 오래된 토큰 캐시 고정

---

## 6) 운영 체크리스트 (두 프로젝트 공통)

- [ ] 토큰 마스터는 Apps Script 단일화
- [ ] import 응답 `ok` 모니터링
- [ ] TTL preflight 기준 통일(작업 단위별 최소 TTL)
- [ ] 토큰 값 원문 로그 금지(sha8/len만 허용)
- [ ] 장애 시 “push 실패”와 “TTL 부족”을 분리 알림

---

## 7) Codex B(반품/리뷰)에게 전달할 한 줄 결론

> “토큰 갱신은 Apps Script 단일 마스터로 유지하고, 반품/리뷰 시스템은 access-only import + TTL preflight 소비자로 붙이면 현재 광고 시스템과 충돌 없이 안정적으로 공존할 수 있다.”

