import json


def write_experiment_log(experiment_log: dict):
    with open("data/experimen_logs.ndjson", "a") as f:
        f.write(json.dumps(experiment_log) + "\n")
