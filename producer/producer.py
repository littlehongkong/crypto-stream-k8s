import json
import time
import logging
import signal
import sys
from datetime import datetime, timezone

import websocket
from confluent_kafka import Producer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = "kafka:29092"
KAFKA_TOPIC     = "crypto-trades"
SYMBOLS         = ["btcusdt", "ethusdt"]

WS_URL = (
    "wss://stream.binance.com:9443/stream?streams="
    + "/".join(f"{s}@trade" for s in SYMBOLS)
)

producer = Producer({
    "bootstrap.servers":        KAFKA_BOOTSTRAP,
    "client.id":                "binance-producer",
    "linger.ms":                5,
    "batch.size":               16384,
    "retries":                  3,
    "retry.backoff.ms":         500,
    "queue.buffering.max.kbytes": 32768,  # 32MB, librdkafka 설정명
})


def delivery_report(err, msg):
    if err:
        log.error(f"Delivery failed | topic={msg.topic()} | {err}")


def parse_trade(raw: dict):
    try:
        data = raw.get("data", {})
        if data.get("e") != "trade":
            return None
        return {
            "symbol":         data["s"],
            "price":          float(data["p"]),
            "quantity":       float(data["q"]),
            "trade_time":     data["T"],
            "trade_time_iso": datetime.fromtimestamp(
                                  data["T"] / 1000, tz=timezone.utc
                              ).isoformat(),
            "is_buyer_maker": data["m"],
            "trade_id":       data["t"],
        }
    except (KeyError, ValueError) as e:
        log.warning(f"Parse error: {e} | raw={raw}")
        return None


message_count = 0

def on_message(ws, message):
    global message_count
    raw = json.loads(message)
    trade = parse_trade(raw)
    if trade is None:
        return

    PARTITION_MAP = {
        "BTCUSDT": 0,
        "ETHUSDT": 1,
    }

    producer.produce(
        topic=KAFKA_TOPIC,
        key=trade["symbol"].encode("utf-8"),
        value=json.dumps(trade).encode("utf-8"),
        partition=PARTITION_MAP.get(trade["symbol"], 2),  # 나머지 심볼은 파티션 2
        callback=delivery_report,
    )

    producer.poll(0)

    message_count += 1
    if message_count % 100 == 0:
        log.info(
            f"[{message_count:>6}건 수신] "
            f"{trade['symbol']} | "
            f"price={trade['price']:,.2f} | "
            f"qty={trade['quantity']:.6f}"
        )


def on_error(ws, error):
    log.error(f"WebSocket error: {error}")


def on_close(ws, close_status_code, close_msg):
    log.info(f"WebSocket closed: {close_status_code} / {close_msg}")
    producer.flush(timeout=10)


def on_open(ws):
    log.info(f"WebSocket connected | symbols={SYMBOLS}")
    log.info(f"Kafka → topic: {KAFKA_TOPIC}")


def handle_signal(sig, frame):
    log.info("종료 중...")
    producer.flush(timeout=15)
    sys.exit(0)

signal.signal(signal.SIGTERM, handle_signal)
signal.signal(signal.SIGINT, handle_signal)


def run():
    retry_delay = 5
    while True:
        try:
            ws = websocket.WebSocketApp(
                WS_URL,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close,
            )
            ws.run_forever(ping_interval=30, ping_timeout=10)
        except Exception as e:
            log.error(f"오류: {e}")
        log.info(f"{retry_delay}초 후 재연결...")
        time.sleep(retry_delay)
        retry_delay = min(retry_delay * 2, 60)


if __name__ == "__main__":
    run()