# Shopee VN Alerts 프로젝트 마스터플랜

## 1. 프로젝트 목적

현재 운영 중인 Shopee 베트남 셀러 시스템에 **서버 배포형 알림 기능**을 추가한다.

1차 목표는 아래 2개만 정확하게 구현하는 것이다.

1. **반품/교환 요청 알림**
   - 고객이 반품/교환/환불 요청을 생성하거나 상태가 바뀌면 직원용 Discord 채널에 알림 전송
2. **리뷰 알림**
   - 고객이 리뷰를 작성하면 직원용 Discord 채널에 알림 전송
   - 별점, 텍스트 내용, 상품 식별 정보가 포함되어야 함

이번 단계에서는 아래는 **의도적으로 제외**한다.

- Apps Script 구조 변경
- 주문/재고 플로우 수정
- 토큰 관리 구조 전면 개편
- 리뷰/반품 이미지·동영상 첨부 미리보기
- 광고 자동화 확장
- Chat, Finance, Ads write 제어 기능 추가

## 2. 현재 운영 전제

- 주문/재고 관리는 **Google Spreadsheet + Apps Script**에서 이미 운영 중이다.
- 서버에는 이미 **광고 리포트 전송 로직**이 존재한다.
- 새 기능은 Apps Script를 건드리지 않고 **서버에 추가 배포**하는 방향으로 간다.
- 토큰 관리는 당장은 기존 Apps Script 쪽 구현을 존중하되, 구조를 먼저 파악한 뒤 이후에 별도 태스크로 정리한다.

## 3. 이번 프로젝트의 핵심 원칙

### 3.1 Apps Script는 유지, 서버가 새 기능을 담당
- 주문/재고 동기화는 기존 흐름 유지
- 신규 알림 로직은 서버에만 추가
- Apps Script는 이번 범위에서 수정하지 않음

### 3.2 Polling first
Shopee 공개 자료 기준으로 이번 두 기능은 webhook-first 보다 **polling-first** 설계가 현실적이다.

- 반품/교환: `returns.getReturnList` → `returns.getReturnDetail`
- 리뷰: `product.getComment` 중심

### 3.3 안정성 우선
이번 1차 목표는 “예쁘게”보다 **놓치지 않는 것**이다.

- 중복 발송 방지
- 폴링 커서 저장
- 장애 시 재시도
- 간단한 헬스체크/로그 확보

### 3.4 토큰은 당장 리팩터링하지 않음
토큰 관리가 명확히 이해되기 전까지는,

- 현재 토큰 공급 구조를 먼저 파악
- 새 기능은 그 구조를 따라가되 결합 지점을 명확히 문서화
- 이후 별도 태스크에서 정리

## 4. 목표 아키텍처

### 4.1 상위 구조

```text
Apps Script (기존 유지)
  └─ 주문/재고 관리

Server (기존 + 신규)
  ├─ 광고 리포트 (기존)
  ├─ Shopee client / auth adapter
  ├─ Returns polling worker
  ├─ Reviews polling worker
  ├─ Discord notifier
  ├─ SQLite/Postgres 상태 저장
  ├─ Scheduler / cron / process manager
  └─ health/logging
```

### 4.2 신규 서버 구성 요소

#### A. Shopee Client Layer
역할:
- 현재 프로젝트에서 이미 사용 중인 Shopee API 호출 방식 재사용
- 서명 로직, 요청 래퍼, 공통 오류 처리 담당
- 토큰 취득 경로는 당장 변경하지 않고 기존 구조를 따라감

#### B. Returns Worker
역할:
- 일정 주기로 반품/교환/환불 요청 조회
- 새 요청/상태변경 감지
- 상세 조회 후 Discord 발송
- 커서 및 fingerprint 저장

#### C. Reviews Worker
역할:
- 일정 주기로 상품 리뷰 조회
- 새 리뷰 감지
- 저별점/일반 리뷰 분기 가능
- Discord 발송

#### D. Discord Notifier
역할:
- webhook 기반 메시지 전송
- 채널별 분기
- 재시도/에러 로깅

#### E. Persistence Layer
역할:
- 마지막 polling cursor
- 이미 보낸 return/review fingerprint
- worker 상태
- shop별 설정

#### F. Health / Logging
역할:
- 마지막 성공 시각
- 마지막 에러
- 폴링 건수/신규 건수
- Discord 발송 실패 기록

## 5. 저장 데이터 최소 설계

## 5.1 shops
```text
shops
- shop_id
- shop_name
- region
- enabled
- discord_returns_webhook_url
- discord_reviews_webhook_url
- timezone
- created_at
- updated_at
```

## 5.2 poll_cursors
```text
poll_cursors
- shop_id
- worker_name         # returns / reviews
- cursor_value
- cursor_meta_json
- updated_at
```

## 5.3 return_events
```text
return_events
- shop_id
- return_sn
- order_sn
- status
- raw_type
- reason
- text_reason
- refund_amount
- due_date
- fingerprint
- payload_json
- first_seen_at
- last_seen_at
- last_notified_at
```

## 5.4 review_events
```text
review_events
- shop_id
- item_id
- model_id
- comment_id
- order_sn
- rating_star
- comment_text
- buyer_username
- ctime
- fingerprint
- payload_json
- first_seen_at
- last_seen_at
- last_notified_at
```

## 6. Worker 전략

### 6.1 Returns Worker
권장 주기:
- **2~5분 간격**

권장 흐름:
1. 각 shop에 대해 `getReturnList` 호출
2. `update_time_from = last_seen - overlap` 방식 사용
3. 신규 또는 상태 변경된 `return_sn` 감지
4. `getReturnDetail` 호출
5. fingerprint 비교
6. Discord 알림
7. cursor 갱신

overlap을 두는 이유:
- 폴링 경계 구간에서 누락 방지

### 6.2 Reviews Worker
리뷰 API는 상품 단위 성격이 강하므로 2단계로 간다.

#### item catalog refresh
권장 주기:
- 6시간~24시간

역할:
- 현재 shop의 item_id 목록 확보
- 활성 상품 기준 캐시 갱신

#### review polling
권장 주기:
- hot set: 10분
- cold set: 6~12시간 순환

hot set 예시:
- 최근 판매량 높은 상품
- 최근 리뷰가 있던 상품
- 광고 집행 중 상품

초기 버전에서는 단순화를 위해:
- 최근 활성 상품 전체를 작은 page로 순회
- 실제 부하를 확인한 뒤 hot/cold 분리 적용

## 7. Discord 알림 설계

### 7.1 반품/교환 알림
필수 필드:
- shop
- return_sn
- order_sn
- status
- raw request type
- reason / text reason
- refund_amount
- due_date

예시:

```text
[반품/교환 요청]
샵: myshop_vn
Return SN: 2403xxxx
Order SN: 2402xxxx
상태: REQUESTED
유형(raw): RETURN_REFUND
사유: DEFECTIVE
상세사유: 상품이 작동하지 않음
환불금액: 189000 VND
기한: 2026-03-07 18:00
```

### 7.2 리뷰 알림
필수 필드:
- shop
- 상품명 또는 item_id
- model_id (있으면)
- order_sn (있으면)
- 별점
- 텍스트
- 작성자
- 작성시각

예시:

```text
[신규 리뷰]
샵: myshop_vn
상품: 휴대용 선풍기 5000mAh
Item ID: 123456789
Model ID: 987654321
주문번호: 2402xxxx
별점: 2/5
작성자: user_***
작성시각: 2026-03-07 14:32
내용: 생각보다 배터리가 빨리 닳아요.
```

### 7.3 채널 분리
권장:
- `#shopee-returns`
- `#shopee-reviews`
- 추후 선택: `#shopee-low-rating`

초기 버전에서는 채널 2개만 있으면 충분하다.

## 8. 토큰 메모 (현 단계 운영 원칙)

현재 이해 기준:
- access token은 짧은 수명 토큰
- refresh token은 더 긴 수명 토큰
- 실제 현재 프로젝트가 어떻게 갱신하고 있는지는 **코드 감사가 먼저 필요**함

현 단계 원칙:
1. 토큰 구조를 당장 바꾸지 않는다.
2. 먼저 현재 Apps Script ↔ 서버 사이의 토큰 흐름을 문서화한다.
3. 신규 worker는 기존 토큰 공급 구조를 최대한 재사용한다.
4. 토큰 race condition 가능성이 보이면 이후 별도 태스크로 분리한다.

## 9. 배포/운영 원칙

### 9.1 권장 배포 위치
기존 광고 리포트가 올라간 **같은 서버** 안에 넣는다.

이유:
- 이미 운영 중인 서버가 있음
- Discord 전송 코드 재사용 가능성
- 배포/로그/모니터링 지점이 한 곳으로 모임

### 9.2 권장 실행 형태
선호 순서:
1. 기존 서버 서비스에 worker 추가
2. 또는 별도 Python service 추가
3. process manager(systemd / supervisor / docker compose)로 상시 실행

### 9.3 초기 DB
초기 버전은 아래 중 하나면 충분하다.
- SQLite
- 기존 서버 DB(Postgres/MySQL)가 있으면 그걸 재사용

권장:
- 프로젝트가 아직 소규모이면 SQLite로 빠르게 시작
- 이미 서버에 DB가 있으면 기존 DB 사용

## 10. 작업 방식 규칙

### 10.1 파일 구조 규칙
프로젝트 안에 아래 폴더를 두고 진행한다.

```text
project/
  tasks/
    TASK-001_*.md
    TASK-002_*.md
  results/
    RESULT-001_*.md
    RESULT-002_*.md
```

### 10.2 Codex 작업 규칙
각 태스크는 아래 원칙을 따른다.

- 태스크 파일을 먼저 읽고 작업
- 결과는 반드시 `results/RESULT-xxx_*.md` 로 남김
- 변경 파일 목록 명시
- 실행한 테스트 명시
- 미확인 사항/리스크 명시
- 사람이 추가로 확인해야 할 점 명시

### 10.3 한 번에 큰 변경 금지
한 태스크에서 아래를 동시에 하지 않는다.
- 토큰 구조 변경
- 신규 worker 2개 동시 구현
- 배포 방식 변경
- DB 마이그레이션 + 서버 구조 개편 + 운영 설정 변경

항상 **작게 나눠서** 진행한다.

## 11. 완료 기준 (1차 릴리즈)

아래가 되면 1차 완료로 본다.

1. 반품/교환 신규 요청 알림이 Discord에 정확히 온다.
2. 반품/교환 상태 변경 알림이 중복 없이 온다.
3. 리뷰 신규 작성 알림이 Discord에 온다.
4. 리뷰 별점과 텍스트가 정확히 나온다.
5. worker 재시작 후에도 cursor가 유지된다.
6. 동일 이벤트 중복 발송이 제어된다.
7. 최소한의 오류 로그와 마지막 성공 시각을 확인할 수 있다.

## 12. 리스크

### 12.1 토큰 공급 구조 불명확
현재 Apps Script가 토큰을 어떻게 유지/갱신하는지 파악 전에는 불안정할 수 있다.

### 12.2 리뷰 API 실제 payload 차이
문서/SDK 예시와 실제 베트남 샵 응답 간 차이가 있을 수 있다.

### 12.3 기존 서버 구조와 충돌 가능성
이미 광고 리포트가 돌아가고 있으므로 스케줄러, 환경변수, 로그 경로 충돌 여부 확인 필요.

### 12.4 과도한 범위 확장 위험
처음부터 저별점 라우팅, 태그 멘션, 시트 기록, 관리자 페이지까지 넣으면 속도가 느려진다.

## 13. Codex 태스크 백로그

### TASK-001
프로젝트 구조 파악 및 현재 토큰/서버/Discord 흐름 감사

### TASK-002
서버 내 알림 모듈 기본 뼈대 생성

### TASK-003
Shopee 공통 클라이언트/설정 계층 정리

### TASK-004
Returns polling worker 구현

### TASK-005
Returns Discord formatter 및 dedupe 구현

### TASK-006
Reviews item catalog 수집 및 review polling 구현

### TASK-007
Reviews Discord formatter 및 dedupe 구현

### TASK-008
배포 설정, 스케줄러, 헬스체크, 로그 정리

### TASK-009
실서버 스모크 테스트 및 운영 검증

### TASK-010
토큰 구조 재점검 및 리팩터링 계획 수립

## 14. 지금 바로 필요한 입력물

다음 중 가능한 것부터 받으면 이후 태스크 정확도가 올라간다.

1. 현재 서버 프로젝트 폴더 구조
2. 광고 리포트 서버가 어떤 언어/프레임워크인지
3. Discord 전송 코드가 이미 있는지
4. Shopee API 호출 공통 모듈이 이미 있는지
5. Apps Script에서 서버로 넘기는 토큰 방식
6. 서버 배포 방식
   - systemd / pm2 / docker / docker compose / nohup 등
7. 환경변수 관리 방식
   - `.env`, secret manager, 수동 export 등

## 15. 다음 액션

지금은 **TASK-001부터 진행**한다.

TASK-001의 목적은 코드를 건드리기보다,
- 지금 프로젝트 구조를 정확히 파악하고
- 어디에 붙여야 가장 안전한지 결정하고
- 토큰/배포/로그/Discord 재사용 가능 지점을 찾아내는 것이다.

그 결과가 나오면, 그 다음 태스크부터 실제 구현으로 들어간다.
