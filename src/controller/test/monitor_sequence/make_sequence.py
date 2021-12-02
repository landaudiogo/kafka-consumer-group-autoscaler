import json
import random

for i in range(0, 30, 5):
    delta = i
    topic_partitions = {
        "delivery_events_v6_topic": [i for i in range(8)],
        "delivery_events_v7_topic": [i for i in range(8)],
    }
    first_measurement = {
        topic: {
            partition: random.randint(0, 100)
            for partition in partitions
        } 
        for topic, partitions in topic_partitions.items()
    }
    measurements = [first_measurement]

    for _ in range(100):
        new_measurement = {
            topic: {
                partition: min(max(speed + random.randint(-1*delta, delta), 0), 100)
                for partition, speed in partitions.items()
            } for topic, partitions in measurements[-1].items()
        }
        shuffle_dict = {}
        for topic, value in new_measurement.items(): 
            value_list = list(value.items())
            random.shuffle(value_list)
            shuffle_dict[topic] = dict(value_list)
        measurements.append(shuffle_dict)

    with open(f"measurements_{delta}", "w") as f:
        json.dump(measurements, f)
        print(f"saved as measurements_{delta}")
