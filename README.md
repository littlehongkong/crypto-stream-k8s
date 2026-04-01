# crypto-stream-k8s

실시간 암호화폐 데이터 파이프라인 — Kafka / PySpark / Kubernetes / Airflow

## 아키텍처
```
Binance WebSocket → Kafka → PySpark Structured Streaming → Postgres + MinIO
                                     ↓
                              Airflow DAG (배치 집계)
                                     ↓
                              Streamlit 대시보드
```

## 기술 스택

| 역할 | 기술 |
|------|------|
| 메시지 브로커 | Apache Kafka 7.5.0 (Confluent) |
| 스트리밍 처리 | PySpark 3.5 Structured Streaming |
| 오케스트레이션 | Apache Airflow 2.x |
| 오브젝트 스토리지 | MinIO (S3 호환) |
| 모니터링 | Streamlit |
| 인프라 | Docker Compose → Minikube (로컬 K8s) |

## 실행 환경

- Apple M1 (arm64), RAM 16GB
- Docker Desktop, Docker Compose v2
- Python 3.11

## 빠른 시작

### 1. Kafka 스택 실행
```bash
docker-compose up -d
```

### 2. 토픽 생성
```bash
bash scripts/create-topics.sh
```

### 3. Producer 실행 (Binance WebSocket)
```bash
docker-compose up -d --build producer
docker-compose logs -f producer
```

### 4. Kafka UI 확인

브라우저에서 http://localhost:8090 접속
Topics → crypto-trades → Messages

## 메시지 스키마 (crypto-trades 토픽)
```json
{
  "symbol":         "BTCUSDT",
  "price":          68104.68,
  "quantity":       0.00008,
  "trade_time":     1743480664000,
  "trade_time_iso": "2026-04-01T04:11:04+00:00",
  "is_buyer_maker": false,
  "trade_id":       123456789
}
```

## 파티션 구성

| 파티션 | 심볼 |
|--------|------|
| 0 | BTCUSDT |
| 1 | ETHUSDT |
| 2 | 예비 (추가 심볼용) |

## 진행 현황

- [x] Day 1-2: Kafka + Producer
- [ ] Day 3-4: PySpark Structured Streaming
- [ ] Day 5: MinIO 연동
- [ ] Day 6-7: Airflow DAG
- [ ] Day 8-9: Minikube 배포
- [ ] Day 10-11: Streamlit 대시보드
