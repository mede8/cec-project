# consumer loop + thread pool + state
# import click idk  yet if we need it or not, if yes add to requirements.txt
import io
import logging
import os
import signal
import db
import notifier
import state

from confluent_kafka import Consumer
from fastavro import reader
from concurrent.futures import ThreadPoolExecutor

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(name)s: %(message)s')
logger = logging.getLogger("consumer")

# env vars
BROKERS = os.getenv("BROKERS", "kafka.cec.dlandau.nl:19092,kafka.cec.dlandau.nl:29092,kafka.cec.dlandau.nl:39092")
TOPIC = os.getenv("TOPIC", "group12")  # check which is it xd
GROUP_ID = os.getenv("GROUP_ID", "groupid12")  # check which is it xd
OFFSET_RESET = os.getenv("OFFSET_RESET", "latest")
SSL_CA_LOCATION = os.getenv("SSL_CA_LOCATION", "./auth/ca.crt")  # # check which is it xd
SSL_KEYSTORE_LOCATION = os.getenv("SSL_KEYSTORE_LOCATION", "./auth/kafka.keystore.pkcs12")  # check which is it xd
SSL_KEYSTORE_PASSWORD = os.getenv("SSL_KEYSTORE_PASSWORD", "cc2023")  # check which is it xd
NOTIFY_WORKERS = int(os.getenv("NOTIFY_WORKERS", "8"))

# c = Consumer({
#     'bootstrap.servers': 'kafka.cec.dlandau.nl:19092,kafka.cec.dlandau.nl:29092,kafka.cec.dlandau.nl:39092',
#     'group.id': f"{random.random()}", # check if this is random
#     'auto.offset.reset': 'latest',
#     'enable.auto.commit': 'true',
#     'security.protocol': 'SSL',
#     'ssl.ca.location': '../auth/ca.crt',
#     'ssl.keystore.location': '../auth/kafka.keystore.pkcs12',
#     'ssl.keystore.password': 'cc2023',
#     'ssl.endpoint.identification.algorithm': 'none',
# })

running = True


def _stop(signum, frame):
    global running
    running = False


def create_consumer() -> Consumer:
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


def get_record_name(msg) -> str:
    headers = dict(msg.headers() or [])
    raw_name = headers.get("record_name")
    return raw_name.decode('utf-8') if isinstance(raw_name, bytes) else raw_name


def process_record(record_name: str, record: dict, conn, pool):
    event = state.handle_event(record_name, record)

    for action, payload in event:
        if action == "save":
            conn = db.save_measurement(conn, **payload)
        elif action == "notify":
            pool.submit(notifier.notify, **payload)
        else:
            logger.warning("unknown action: %s", action)
    return conn


# @click.command()
# @click.argument('topic')
# def consume(topic: str):
def consume():
    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    conn = db.connect()
    pool = ThreadPoolExecutor(max_workers=NOTIFY_WORKERS)

    consumer = create_consumer()
    consumer.subscribe([TOPIC], on_assign=lambda _, p_list: logger.info("assigned: %s", p_list))
    logger.info("subscribed to %s, groupid %s", TOPIC, GROUP_ID)

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
                print(records)
                for record in records:
                    print(record)
            except Exception as e:
                logger.error("fail to decode %s: %s", record_name, e)
                continue

            print(record_name)  # remove later
            for record in records:
                try:
                    conn = process_record(record_name, record, conn, pool)
                    print(record)  # remove later
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
