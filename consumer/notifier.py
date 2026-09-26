import requests


def notify(notification_type: str, experiment_id: str, researcher: str, measurement_id: str, chiper_data: str) -> None:
    """Sends a notification to the specified endpoint with the provided data.

    :param notification_type: the type of notification to send.
    :param experiment_id: the ID of the experiment.
    :param researcher: name of researcher.
    :param measurement_id: ID of the measurement.
    :param chiper_data: data to send in the notification-service.
    """
    requests.post(
        "http://notification-service:3000/notify",  # this needs to be checked
        json={
            "notification_type": notification_type,
            "experiment_id": experiment_id,
            "researcher": researcher,
            "measurement_id": measurement_id,
            "chiper_data": chiper_data
        },
        timeout=5
    )
