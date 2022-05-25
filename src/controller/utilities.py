import time
import os
import json

from os.path import isfile, join
from typing import List, Set

from exc import (
    StopwatchExists, StopwatchTerminated, MissingStopwatch
)

def file_generator(): 
    base_dir = "./test/monitor_sequence"
    list_file_names = [
        f 
        for f in os.listdir(base_dir) 
        if (isfile(join(base_dir, f)) 
            and join(base_dir, f) not in [join(base_dir, "make_sequence.py"),
                                          join(base_dir, ".gitignore")]
        )
    ]
    print(list_file_names)
    for file_name in list_file_names: 
        yield file_name
        
def algorithm_generator(): 
    algorithms = ["mwf", "mwfp", "mbfp", "nf", "nfd", "ff", "ffd", "wf", "wfd", "bf", "bfd", "mbf"]
    for a in algorithms: 
        yield a

def clean_up_measurement_files(): 
    base_dir = 'test/monitor_sequence'
    list_file_names = [
        f 
        for f in os.listdir(base_dir) 
        if (isfile(join(base_dir, f)) 
            and join(base_dir, f) not in [join(base_dir, "make_sequence.py"),
                                          join(base_dir, ".gitignore")]
        )
    ]
    for file_name in list_file_names: 
        os.remove(join(base_dir, file_name))


class LogStopwatch:
    """Class to handle multiple stopwatches at a time."""


    def __init__(self, cols: Set[str] = None): 
        self.deltas = {}
        self.historic = {}
    
    def start(self, key): 
        if key in self.deltas:
            raise StopwatchExists()
        self.deltas[key] = [time.time(), None]

    def stop(self, key): 
        if key not in self.deltas: 
            raise MissingStopwatch()
        self.deltas[key][1] = time.time()

    def toggle(self, key): 
        if key not in self.deltas: 
            self.start(key)
        else: 
            if self.deltas[key][1] != None: 
                raise StopwatchTerminated()
            self.deltas[key][1] = time.time()

    def commit(self): 
        for key, [start, stop] in self.deltas.items():
            if key in self.historic:
                self.historic[key].append(stop-start)
            else: 
                self.historic[key] = [stop-start]
        self.deltas = {}

    def started(self, key): 
        return True if key in self.deltas else False

    def save_to_file(self, directory): 
        directory = os.path.abspath(directory)
        for key, value in self.historic.items():
            with open(f"{directory}/{key}", "w") as fp:
                json.dump(value, fp)
