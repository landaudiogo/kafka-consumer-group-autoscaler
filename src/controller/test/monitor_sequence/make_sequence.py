import json
import random

delta = random.randint(0,30)
topic_partitions = {
    "delivery_events_v6_topic": [i for i in range(16)],
    "delivery_events_v7_topic": [i for i in range(16)],
}
first_measurement = {
    topic: {
        partition: random.randint(0, 100)
        for partition in partitions
    } 
    for topic, partitions in topic_partitions.items()
}

measurements = [first_measurement]

for _ in range(99):
    new_measurement = {
        topic: {
            partition: min(max(speed + random.randint(-1*delta, delta), 0), 100)
            for partition, speed in partitions.items()
        } for topic, partitions in measurements[-1].items()
    }
    measurements.append(new_measurement)

with open(f"measurements_{delta}", "w") as f:
    json.dump(measurements, f)
    print(f"saved as measurements_{delta}")
