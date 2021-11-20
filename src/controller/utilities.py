import time
import os
import json

from os.path import isfile, join

def file_generator(): 
    base_dir = "./test/monitor_sequence"
    list_file_names = [
        f 
        for f in os.listdir(base_dir) 
        if (isfile(join(base_dir, f)) 
            and join(base_dir, f) != join(base_dir, "make_sequence.py")
        )
    ]
    print(list_file_names)
    for file_name in list_file_names: 
        with open(join(base_dir, file_name), "r") as f:
            print(file_name)
            yield file_name, json.load(f)
        

def algorithm_generator(): 
    # algorithms = ["mwf", "bfd", "ffd", "wfd", "nfd", "bf", "ff", "nf"]
    algorithms = ["mbf", "wf", "wfd", "mwf", "ffd", "ff", "bfd", "bf"]
    for a in algorithms: 
        yield a



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

    def current_timediff(self, code=None):
        if code == None: 
            return self.current_timestamp()
        return (self.current_tstamp() - self.last_tstamp(code) 
            if self.last_tstamp(code) != None
            else None
        )
