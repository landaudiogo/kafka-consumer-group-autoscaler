import json
import random

from os.path import join


def generate_measurements(
    delta_start, delta_end, delta_diff, Npartitions, delta_offset,
    Nmeasurements, start_speed=None, save_dir="./test/monitor_sequence"
):
    for delta in range(delta_start, delta_end, delta_diff):
        topic_partitions = {
            "delivery_events_v6_topic": [i for i in range(Npartitions//2)],
            "delivery_events_v7_topic": [i for i in range(Npartitions//2)],
        }
        first_measurement = {
            topic: {
                partition: (random.randint(0, 100) 
                    if start_speed is None else start_speed
                )
                for partition in partitions
            } 
            for topic, partitions in topic_partitions.items()
        }
        measurements = [first_measurement]

        for _ in range(Nmeasurements):
            new_measurement = {
                topic: {
                    partition: min(max(
                        speed + random.randint(0, delta_offset) + random.randint(-1*delta, delta), 0
                    ), 100)
                    for partition, speed in partitions.items()
                } for topic, partitions in measurements[-1].items()
            }
            shuffle_dict = {}
            for topic, value in new_measurement.items(): 
                value_list = list(value.items())
                random.shuffle(value_list)
                shuffle_dict[topic] = dict(value_list)
            measurements.append(shuffle_dict)

        with open(join(save_dir, f"measurements_{delta}"), "w") as f:
            json.dump(measurements, f)
            print(f"saved as measurements_{delta}")


if __name__ == '__main__': 
    generate_measurements(
        0, 30, 5, 200, 0, 500, start_speed=0
    )
