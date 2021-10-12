from confluent_kafka import Producer
import time
import sys


class Timeline:
    def __init__(self): 
        self.start = time.time()
        self.chronology = [[0, 'Beginning']]
        self.coded_tstamps = {}

    def add(self, key, code=None):
        ctime = time.time()-self.start
        if code != None: 
            if self.coded_tstamps.get(code): 
                self.coded_tstamps[code].append([ctime, key])
            else: 
                self.coded_tstamps[code] = [[ctime, key]]
        self.chronology.append([ctime, key])
            
    def print(self):
        for tstamp in self.chronology: 
            print(f"{tstamp[0]},{tstamp[1]}")

    def print_code(self, code): 
        for tstamp in self.coded_tstamps[code]:
            print(f"{tstamp[0]},{tstamp[1]}")

    def current_tstamp(self): 
        return time.time() - self.start

    def last_tstamp(self, code): 
        lst = self.coded_tstamps.get(code)
        if lst: 
            return lst[-1][0]


producer_conf = {
    'bootstrap.servers': 'broker:29092', 
    'client.id': 'new_producer', 
}
producer = Producer(producer_conf)
timeline = Timeline()

test_str = 'Test message with as many bytes as this message is long'

delta_times = [1] * 35
delta_times.extend([0.75]*47)
delta_times.extend([0.5]*70)

timeline.add(f'New Iteration', code=0)

while len(delta_times):

    time_diff = timeline.current_tstamp() - timeline.last_tstamp(0)
    if(time_diff > delta_times[0]): 
        delta_times.pop(0)
        producer.produce('monitor_speed_test', test_str.encode())
        producer.flush()

        time_diff = timeline.current_tstamp() - timeline.last_tstamp(0)
        speed = 123/time_diff
        producer.produce('producer-speed', str(speed).encode())
        producer.flush()
        print(speed)
        timeline.add(f'New Iteration', code=0)
    else: 
        time.sleep(0.01)


