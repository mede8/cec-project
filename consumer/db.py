"""Thin Postgres layer for the consumer that saves the measurements and connects to the database."""
import logging
import os
import time
import psycopg2


logger = logging.getLogger(__name__)

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/postgres")

_INSERT_SQL = """
    INSERT INTO temperatures (experiment_id, measurement_id, ts, temperature, out_of_range)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (experiment_id, measurement_id) DO NOTHING
"""


def connect(url: str = DATABASE_URL, retries: int = 30, delay: float = 2.0) -> psycopg2.extensions.connection:
    """Connects the consumer to the database.

    :param url: the URl to access the Postgres database.
    :param retried: number of retries for connecting to the database.
    :param delay: delay time if the attempt to connect failed.
    :return: the connection to the database.
    """
    last_error = None

    for attempt in range(1, retries+1):
        try:
            conn = psycopg2.connect(url)
            conn.autocommit = True
            logger.info("connected to db (attempt %d)", attempt)
            return conn
        except psycopg2.OperationalError as e:
            last_error = e
            logger.warning("db not ready (attempt %d/%d): %s", attempt, retries, str(e).strip())
            time.sleep(delay)

    raise RuntimeError(f"failed to connect to the db after {retries} attempts") from last_error


def save_measurement(
        conn: psycopg2.extensions.connection, experiment_id: str, measurement_id: str,
        timestamp: str, temperature: float, out_of_range: bool) -> psycopg2.extensions.connection:
    """Saves the averaged measurement in the database.

    :param conn: the connection to the database.
    :param experiment_id: ID of the current experiment.
    :param measurement_id: ID of the measurement.
    :param timestamp: the time of the experiment configuration.
    :param temperature: the value of the temperature to save.
    :param out_of_range: a boolean that determines whether the temperature is out-of-range.
    :return: the connection
    """
    params = (experiment_id, measurement_id, timestamp, temperature, out_of_range)

    try:
        with conn.cursor() as cur:
            cur.execute(_INSERT_SQL, params)
    except (psycopg2.OperationalError, psycopg2.InterfaceError):
        logger.warning("db connection error")
        conn = connect()
        with conn.cursor() as cur:
            cur.execute(_INSERT_SQL, params)
    return conn
