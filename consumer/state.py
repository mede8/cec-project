"""
Notebook file that contains the state of the consumer and the experiments criteria.
"""
experiments = {}


def handle_event(record_name: str, event: dict) -> list:
    """Handles an event by updating the experiments state.

    :param record_name: the name of the record associated with the event.
    :param event: the event data to process.
    """
    todo = []
    exp_id = event["experiment"]

    if record_name == "experiment_configured":
        temp_range = event["temperature_range"]
        experiments[exp_id] = {
            "researcher": event["researcher"],
            "sensors": event["sensors"],
            "lower": temp_range["lower_threshold"],
            "upper": temp_range["upper_threshold"],
            "stabilized_notified": False,
            "was_out_of_range": False,
            "pending_readings": {}
        }
        experiments[exp_id]["phase"] = "configured"

    elif record_name == "stabilization_started":
        exp = experiments.get(exp_id)
        if exp:
            exp["phase"] = "stabilization"

    elif record_name == "experiment_started":
        exp = experiments.get(exp_id)
        if exp:
            exp["phase"] = "running"

    elif record_name == "sensor_temperature_measured":
        exp = experiments.get(exp_id)
        if exp is None or exp["phase"] not in ("stabilization", "running"):
            return todo

        mid = event.get("measurement_id") or event.get("measurement-id")
        slot = exp["pending_readings"].setdefault(
            mid, {"readings": {}, "timestamp": event["timestamp"], "hash": event["measurement_hash"]}
        )
        slot["readings"][event["sensor"]] = event["temperature"]

        if len(slot["readings"]) < len(exp["sensors"]):
            # wait until every sensor of this experiment is reported
            return todo

        del exp["pending_readings"][mid]
        avg = sum(slot["readings"].values()) / len(slot["readings"])
        in_range = exp["lower"] <= avg <= exp["upper"]

        if exp["phase"] == "stabilization":
            # notify the researcher once when it first reaches range
            if not exp["stabilized_notified"] and in_range:
                exp["stabilized_notified"] = True
                todo.append((
                    "notify", {
                        "notification_type": "Stabilized",
                        "researcher": exp["researcher"],
                        "experiment_id": exp_id,
                        "measurement_id": mid,
                        "cipher_data": slot["hash"]
                    }
                ))

        elif exp["phase"] == "running":
            # notify the researcher when the temperature goes out of range
            todo.append((
                 "save", {
                     "experiment_id": exp_id,
                     "measurement_id": mid,
                     "timestamp": slot["timestamp"],
                     "temperature": avg,
                     "out_of_range": not in_range
                 }
             ))
            if not in_range and not exp.get("was_out_of_range"):
                todo.append((
                    "notify", {
                        "notification_type": "OutOfRange",
                        "researcher": exp["researcher"],
                        "experiment_id": exp_id,
                        "measurement_id": mid,
                        "cipher_data": slot["hash"]
                    }
                ))
            exp["was_out_of_range"] = not in_range

    elif record_name == "experiment_terminated":
        exp = experiments.get(exp_id)
        if exp:
            exp["phase"] = "terminated"
        del experiments[exp_id]

    return todo
