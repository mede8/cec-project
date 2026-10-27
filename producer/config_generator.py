import json
import random
import sys


n = int(sys.argv[1]) if len(sys.argv) > 1 else 100
exps = []

for i in range(n):
    lower = round(random.uniform(20, 30), 1)
    exps.append({
        "start_time": i * 2,
        "researcher": "test@uu.nl",
        "num_sensors": random.randint(2, 5),
        "sample_rate": 1000,
        "start_temperature": 16,
        "stabilization_samples": 10,
        "carry_out_samples": 300,
        "temp_range": {"lower_threshold": lower, "upper_threshold": lower + 2}
    })
json.dump(exps, open("config.json", "w"), indent=2)
print(f"wrote {n} experiments to config.json")
