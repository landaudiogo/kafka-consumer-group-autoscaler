import matplotlib.pyplot as plt
import numpy as np
import json


with open('monitor.log', 'r') as f:
    monitor_logs = json.load(f)
with open('producer.log', 'r') as f:
    producer_logs = json.load(f)


monitor_y, monitor_x = zip(*monitor_logs)
producer_y, producer_x = zip(*producer_logs)

plt.plot(monitor_x, monitor_y)
plt.plot(producer_x, producer_y)
plt.title('Write Speed to Partition over time')
plt.xlabel('time (s)')
plt.ylabel('write speed (bytes/s)')
plt.show()
