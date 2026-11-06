"""Consumer script for reading and decoding Kafka messages."""
import io
import logging
import os
import signal
import db
import notifier
import state
import time
from confluent_kafka import Consumer
from fastavro import reader
from concurrent.futures import ThreadPoolExecutor


logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(name)s: %(message)s')
logger = logging.getLogger("consumer")

# env vars
BROKERS = os.getenv("BROKERS", "")
TOPIC = os.getenv("TOPIC", "")
GROUP_ID = os.getenv("GROUP_ID", "")
OFFSET_RESET = os.getenv("OFFSET_RESET", "latest")
SSL_CA_LOCATION = os.getenv("SSL_CA_LOCATION", "")
SSL_KEYSTORE_LOCATION = os.getenv("SSL_KEYSTORE_LOCATION", "")
SSL_KEYSTORE_PASSWORD = os.getenv("SSL_KEYSTORE_PASSWORD", "")
NOTIFY_WORKERS = int(os.getenv("NOTIFY_WORKERS", "8"))


running = True


def _stop(signum, frame):
    global running
    running = False


def create_consumer() -> Consumer:
    """Create and return a Kafka consumer with the specified configuration."""
    c = Consumer({
        'bootstrap.servers': BROKERS,
        'group.id': GROUP_ID,
        'auto.offset.reset': OFFSET_RESET,
        'enable.auto.commit': 'true',
        'security.protocol': 'SSL',
        'ssl.ca.location': SSL_CA_LOCATION,
        'ssl.keystore.location': SSL_KEYSTORE_LOCATION,
        'ssl.keystore.password': SSL_KEYSTORE_PASSWORD,
        'ssl.endpoint.identification.algorithm': 'none',
    })

    return c


def get_record_name(msg: dict) -> str:
    """Extract the record name from the Kafka message headers.
    
    :param msg: the Kafka message.
    :return: the record name as a string.
    """
    headers = dict(msg.headers() or [])
    raw_name = headers.get("record_name")
    return raw_name.decode('utf-8') if isinstance(raw_name, bytes) else raw_name


def process_record(
        record_name: str, record: dict, conn: db.Connection, pool: ThreadPoolExecutor
) -> db.Connection:
    """Process a single record, update the database and send notificaitons if needed.
    
    :param record_name: the name of the record.
    :param record: the record data as a dictionary.
    :param conn: the database connection.
    :param pool: the thread pool executor for sending notifications.
    :return: the updated database connection.
    """
    event = state.handle_event(record_name, record)

    for action, payload in event:
        if action == "save":
            conn = db.save_measurement(conn, **payload)
        elif action == "notify":
            pool.submit(notifier.notify, **payload)
        else:
            logger.warning("unknown action: %s", action)
    return conn


def consume():
    """Poll the Kafka topic, decode Avro messages, and process them in a thread pool."""
    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    conn = db.connect()
    pool = ThreadPoolExecutor(max_workers=NOTIFY_WORKERS)

    consumer = create_consumer()
    consumer.subscribe([TOPIC], on_assign=lambda _, p_list: logger.info("assigned: %s", p_list))
    logger.info("subscribed to %s, groupid %s", TOPIC, GROUP_ID)

    # for the lag duration
    lags = []
    last_lag_log = time.time()

    try:
        while running:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error("consumer error: %s", msg.error())
                continue

            record_name = get_record_name(msg)
            try:
                records = list(reader(io.BytesIO(msg.value())))
                # print(records)
            except Exception as e:
                logger.error("fail to decode %s: %s", record_name, e)
                continue

            for record in records:
                if record_name == "sensor_temperature_measured":
                    now = time.time()
                    lags.append(now - record['timestamp'])
                    if now - last_lag_log >= 5:
                        lags.sort()
                        logger.info("lag avg=%.3fs p95=%.3fs max=%.3fs n=%d",
                                    sum(lags) / len(lags), lags[int((len(lags) - 1) * 0.95)], lags[-1], len(lags))
                        lags = []
                        last_lag_log = now
                try:
                    conn = process_record(record_name, record, conn, pool)
                except Exception as e:
                    logger.error("failed to process %s: %s (%s)", record_name, record, e)
    finally:
        logger.info("closing consumer")
        consumer.close()
        pool.shutdown(wait=True)
        conn.close()


# consume()
if __name__ == "__main__":
    consume()
