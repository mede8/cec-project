"""
2 pytest fixtures (reset_state and base_config_event)
test_experiment_configured (correctlt initalised)
test_phase_transitions (stabilization and running)
test_sensor_aggregation_waits_for_all_sensors (no action until all sensors report)
test_stabilization_notification_triggers_once (notify only first time in range)
test_running_phase_saves_and_notifies_out_of_range (saves all data and notifies when out of range)
test_measurement_id_fallback_key (both measurement_id and measurement-id keys)
test_ignored_measurements_wrong_phase (measurements ignored if not running or stabilizing)
test_experiment_terminated (terminating an experiment removes it from state)
"""