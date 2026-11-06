"""Sends notifications to the notifications-service"""
import requests
import os
import time
import logging
from typing import Optional


logger = logging.getLogger(__name__)
NOTIFY_URL = os.getenv("NOTIFY_URL", "http://host.docker.internal:3000/api/notify")
NOTIFY_TOKEN = os.getenv("NOTIFY_TOKEN", "")
_session = requests.Session()


def notify(notification_type: str, experiment_id: str, researcher: str, measurement_id: str, cipher_data: str,
           timestamp: Optional[float] = None) -> None:
    """Sends a notification to the specified endpoint with the provided data.

    :param notification_type: the type of notification to send.
    :param experiment_id: the ID of the experiment.
    :param researcher: name of researcher.
    :param measurement_id: ID of the measurement.
    :param chiper_data: data to send in the notification-service.
    :param timestamp: optional timestamp of the notification.
    """
    params = {"token": NOTIFY_TOKEN} if NOTIFY_TOKEN else None
    body = {
        "notification_type": notification_type,
        "experiment_id": experiment_id,
        "researcher": researcher,
        "measurement_id": measurement_id,
        "cipher_data": cipher_data
    }
    for attempt in range(3):
        sent = time.time()
        try:
            response = _session.post(NOTIFY_URL, params=params, json=body, timeout=5)
            response.raise_for_status()
            done = time.time()
            latency = f"{(done - timestamp) * 1000:.0f}" if timestamp else "-"
            logger.info("notify %s %s %s -> %s latency=%sms post=%.0fms",
                        notification_type, experiment_id, measurement_id,
                        response.status_code, latency, (done - sent) * 1000)
            return
        except requests.RequestException as e:
            logger.warning("notify attempt %d failed: %s", attempt + 1, e)
            time.sleep(0.2 * (attempt + 1))
    logger.error("giving up on notification %s/%s", experiment_id, measurement_id)
