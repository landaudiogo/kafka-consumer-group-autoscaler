import time

from typing import Callable
from config import MAX_TIME_S1, CONSUMER_CAPACITY
from dstructures import (
    TopicPartitionConsumer, ConsumerList
)
from approximation_algorithms import (
    AlgorithmFactory
)

class Transition: 


    def __init__(self, trigger_function, origin, destination): 
        self.origin = origin
        self.destination = destination
        self.trigger_function = trigger_function

    def __call__(self):
        if(self.trigger_function() == True): 
            return self.destination



class State:
    
    def __init__(self, controller): 
        self.controller = controller
        self.transitions = []

    def entry(self):
        print("==== ", self.__class__.__name__, ' ====')
        self.start_time = time.time()

    def elapsed_time(self): 
        if self.start_time == None:
            raise Exception()
        return time.time() - self.start_time

    def exit(self): 
        self.start_time = None

    def execute(self): 
        pass

    def add_transition(
        self, 
        transition: Transition
    ): 
        self.transitions.append(transition)

    def verify_transitions(self): 
        for transition in self.transitions:
            dest = transition()
            if dest != None: 
                return dest
        return None



class StateSentinel(State): 


    def time_up(self):
        return self.elapsed_time() > MAX_TIME_S1

    def full_bin(self):
        for c in self.controller.consumer_list: 
            if c == None: 
                continue
            if c.combined_speed > CONSUMER_CAPACITY:
                return True
        return False
            
    def any_unassigned(self): 
        return not (len(self.controller.unassigned_partitions) == 0)

    def entry(self): 
        super().entry()
        self.controller.unassigned_partitions = []

    def execute(self): 
        partition_speeds = self.controller.get_last_monitor_record()
        # print(partition_speeds)
        for topic_name, p_speeds in partition_speeds.items(): 
            for p_str, speed  in p_speeds.items():
                speed = min(CONSUMER_CAPACITY, speed)
                p_int = int(p_str)
                tp = TopicPartitionConsumer(topic_name, p_int)

                consumer = self.controller.consumer_list.get_consumer(tp)
                if consumer == None:
                    tp.update_speed(speed)
                    self.controller.unassigned_partitions.append(tp)
                else:
                    consumer.update_partition_speed(tp, speed)


class StateReassignAlgorithm(State): 

    
    def __init__(self, *args, **kwargs): 
        super().__init__(*args)
        self.ALGORITHM_STATUS = None
        self.algorithm = AlgorithmFactory.get_algorithm(
            kwargs.get("approximation_algorithm", "bfd")
        )

    def entry(self): 
        super().entry()
        self.ALGORITHM_STATUS = False

    def exit(self): 
        super().exit()
        self.ALGORITHM_STATUS = None

    def execute(self): 
        self.controller.next_assignment = self.algorithm.run(
            self.controller.consumer_list,
            self.controller.unassigned_partitions
        )
        self.ALGORITHM_STATUS = True

    def finished_approximation_algorithm(self): 
        if self.ALGORITHM_STATUS != None:
            return self.ALGORITHM_STATUS
        else: 
            raise Exception()


class StateGroupManagement(State): 
    def __init__(self, controller):
        super().__init__(controller)
        self.FINAL_GROUP_STATE = None

    def entry(self): 
        super().entry()
        self.FINAL_GROUP_STATE = False

    def exit(self):
        super().exit()
        self.FINAL_GROUP_STATE = None

    def group_reached_state(self): 
        if self.FINAL_GROUP_STATE == None:
            raise Exception()
        return self.FINAL_GROUP_STATE

    def execute(self): 
        delta = self.controller.next_assignment - self.controller.consumer_list 
        # print(list(res.batch.values())[0].to_record_list())
        # self.controller.create_consumers(delta.consumers_create)
        self.controller.change_consumers_state(delta)
        self.FINAL_GROUP_STATE = True




class StateMachine:


    def __init__(self, controller, states=[], transitions=[]): 
        self.controller = controller
        self.initial = None
        self.current_state = None
        self.states = {key: st for key, st in states}
        for orig, dest, f in transitions: 
            sorig = self.states.get(orig)
            if sorig == None:
                raise Exception()

            sdest = self.states.get(dest)
            if sdest == None:
                raise Exception()

            t = Transition(f, sorig, sdest)
            sorig.add_transition(t)

    def add_state(self, key: str, state: State): 
        self.states[key] = state

    def add_transition(
        self, 
        function_transition: Callable[[], bool], 
        origin_key: str,
        destination_key, str
    ):
        sorigin = self.states.get(origin_key)
        if sorigin == None: 
            raise Exception()

        sdestination = self.state.get(destination_key)
        if sdestination == None:
            raise Exception()

        t = Transition(function_transition, sorigin, sdestination)
        orig.add_transition(t)

    def set_initial(self, key):
        s = self.states.get(key)
        if s == None: 
            raise Exception()
        self.initial = s

    def execute(self): 
        if self.current_state == None: 
            self.initialize()
        self.current_state.execute()

        sdest = self.current_state.verify_transitions()
        if sdest != None: 
            self.change_state(sdest)

    def change_state(self, destination):
        self.current_state.exit()
        self.current_state = destination
        self.current_state.entry()

    def initialize(self): 
        if self.initial == None: 
            raise Exception()
        print("==== Starting State Machine ====")
        self.current_state = self.initial
        self.current_state.entry()
