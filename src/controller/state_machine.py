import time
import functools
import logging
import csv
import os

from typing import Callable
from config import (
    MAX_TIME_S1, CONSUMER_CAPACITY, ALGO_CAPACITY,
    MAX_TIME_STATE_GM, STATE_MACHINE_LOGGER_LEVEL
)
from dstructures import (
    TopicPartitionConsumer, ConsumerList
)
from approximation_algorithms import (
    AlgorithmFactory
)
from exc import (
    StopMeasurementIteration, SwitchAlgorithm, NextFile, InactiveState,
    MissingInitialState, NoMoreFiles
)
from test.monitor_sequence.make_sequence import generate_measurements
from utilities import (
    file_generator, algorithm_generator, clean_up_measurement_files
)


logger = logging.getLogger(__name__)
logger.setLevel(STATE_MACHINE_LOGGER_LEVEL)


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
        logger.debug(self)
        self.start_time = time.time()

    def elapsed_time(self): 
        if self.start_time == None:
            raise InactiveState()
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

    def __repr__(self): 
        return f'=== {self.__class__.__name__} ==='


class StateSentinel(State): 


    def __init__(self, *args, **kwargs):
        self.ITERATION = 0
        super().__init__(*args, **kwargs)

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

    def exit(self):
        super().exit()
        self.ITERATION += 1

    def execute(self): 
        partition_speeds = self.controller.get_last_monitor_record()
        if partition_speeds == None: 
            return
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

    def __repr__(self): 
        return f'=== StateSentinel {self.ITERATION} ==='


class StateSentinelStep(State): 


    def __init__(self, *args, **kwargs):
        self.ITERATION = 0
        super().__init__(*args, **kwargs)

    def any_unassigned(self): 
        return not (len(self.controller.unassigned_partitions) == 0)

    def time_up(self):
        return self.elapsed_time() > MAX_TIME_S1

    def full_bin(self):
        for c in self.controller.consumer_list: 
            if c == None: 
                continue
            if (c.combined_speed > ALGO_CAPACITY) and (len(c.partitions()) > 1):
                return True
        return False
            
    def entry(self): 
        super().entry()
        self.controller.unassigned_partitions = []

    def exit(self):
        super().exit()
        if self.controller.log_stopwatch.started("t1"): 
            self.controller.log_stopwatch.toggle("t1")
        self.ITERATION += 1

    def execute(self): 
        partition_speeds = self.controller.get_last_monitor_record()
        if partition_speeds == None: 
            return
        for topic_name, p_speeds in partition_speeds.items(): 
            for p_str, speed  in p_speeds.items():
                speed = min(CONSUMER_CAPACITY, speed)
                p_int = int(p_str)
                if ((speed > 0) and (speed < ALGO_CAPACITY) and
                    not self.controller.log_stopwatch.started("t1")
                ): 
                    self.controller.log_stopwatch.toggle("t1")

                tp = TopicPartitionConsumer(topic_name, p_int)

                consumer = self.controller.consumer_list.get_consumer(tp)
                if consumer == None:
                    tp.update_speed(speed)
                    self.controller.unassigned_partitions.append(tp)
                else:
                    consumer.update_partition_speed(tp, speed)

    def __repr__(self): 
        return f'=== StateSentinel {self.ITERATION} ==='

class StateSentinelFile(State): 
    """This state sentinel is used when the speed measurements are to be fetched
    from a file, instead of using the data-engineering-monitor topic.

    """

    def __init__(self, *args, **kwargs):
        generate_measurements(
            5, 6, 1, 32, 0, 100, start_speed=None
        )
        self.file = next(file_generator())
        self.ITERATION = 0
        super().__init__(*args, **kwargs)

    def next_file(self): 
        logger.info("Terminated")
        clean_up_measurement_files()
        print("Copy Files")
        time.sleep(60*60*2)
        exit(0)

    def time_up(self):
        return self.elapsed_time() > 0.3

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

    def exit(self):
        super().exit()
        self.ITERATION += 1

    def execute(self): 
        try: 
            partition_speeds = self.controller.get_file_measurement(self.file)
        except StopMeasurementIteration as e:
            raise SwitchAlgorithm()
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

    def __repr__(self): 
        return f'=== StateSentinel {self.ITERATION} ==='


class StateSentinelAlgorithms(State): 


    def __init__(self, *args, **kwargs):
        # generate_measurements(
        #     0, 30, 5, 200, 0, 500
        # )
        self.file_generator = file_generator()
        self.file = None
        self.next_file()
        self.ITERATION = 0
        super().__init__(*args, **kwargs)

    def next_file(self): 
        try:
            self.file = next(self.file_generator)
            logger.info(f"New measurements file - {self.file}")
        except:
            logger.info("No more files")
            clean_up_measurement_files()
            raise NoMoreFiles()

    def time_up(self):
        return True

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

    def exit(self):
        super().exit()
        self.ITERATION += 1

    def execute(self): 
        try: 
            partition_speeds = self.controller.get_file_measurement(self.file)
        except StopMeasurementIteration as e:
            self.ITERATION = -1
            self.controller.consumer_list = ConsumerList()
            raise SwitchAlgorithm()

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

    def __repr__(self): 
        return f'=== StateSentinel {self.ITERATION} ==='


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
        # self.controller.log_stopwatch.toggle("t2")

    def exit(self): 
        super().exit()
        self.ALGORITHM_STATUS = None

    def execute(self): 
        self.algorithm.treat_input(
            self.controller.consumer_list, self.controller.unassigned_partitions
        )
        self.controller.next_assignment = self.algorithm.run()
        self.ALGORITHM_STATUS = True

    def finished_approximation_algorithm(self): 
        if self.ALGORITHM_STATUS != None:
            return self.ALGORITHM_STATUS
        else: 
            raise InactiveState()


class StateReassignAlgorithmTest(State): 
    """State to be used in conjunction with StateSentinelAlgorithms or
    StateSentinelFile to test all the algorithms for a file.

    """

    
    def __init__(self, *args, **kwargs): 
        super().__init__(*args)
        self.ALGORITHM_STATUS = None
        self.algorithm_generator = algorithm_generator()
        self.algorithm_name = None
        self.algorithm = None
        self.next_algorithm()

    def next_algorithm(self): 
        try: 
            self.algorithm_name = next(self.algorithm_generator)
            self.algorithm = AlgorithmFactory.get_algorithm(self.algorithm_name)
            logger.info(f"New approximation algorithm - {self.algorithm_name}")
        except Exception as e: 
            print(e)
            self.algorithm_generator = algorithm_generator()
            self.next_algorithm()
            raise NextFile()

    def entry(self): 
        super().entry()
        self.ALGORITHM_STATUS = False
        # self.controller.log_stopwatch.toggle("t2")

    def exit(self): 
        super().exit()
        self.ALGORITHM_STATUS = None

    def execute(self): 
        self.algorithm.treat_input(
            self.controller.consumer_list, self.controller.unassigned_partitions
        )
        self.controller.next_assignment = self.algorithm.run()
        self.ALGORITHM_STATUS = True

    def finished_approximation_algorithm(self): 
        if self.ALGORITHM_STATUS != None:
            return self.ALGORITHM_STATUS
        else: 
            raise InactiveState()




class StateGroupManagement(State): 
    def __init__(self, controller):
        super().__init__(controller)
        self.FINAL_GROUP_STATE = None
        self.OUT_OF_SYNC = None

    def entry(self): 
        super().entry()
        self.FINAL_GROUP_STATE = False
        self.OUT_OF_SYNC = False

    def exit(self):
        super().exit()
        self.FINAL_GROUP_STATE = None
        self.OUT_OF_SYNC = None

    def group_reached_state(self): 
        if self.FINAL_GROUP_STATE == None:
            raise InactiveState()
        return self.FINAL_GROUP_STATE

    def out_of_sync(self): 
        if self.OUT_OF_SYNC == None:
            raise InactiveState()
        return self.OUT_OF_SYNC

    def execute(self): 
        try:
            delta = self.controller.next_assignment - self.controller.consumer_list 
            self.controller.create_consumers()
            self.controller.log_stopwatch.toggle("t2")
            self.controller.log_stopwatch.toggle("t3")
            self.controller.wait_deployments_ready()
            self.controller.log_stopwatch.toggle("t3")
            self.controller.log_stopwatch.toggle("t4")
            self.controller.change_consumers_state(delta)
            self.controller.log_stopwatch.toggle("t4")
            self.controller.log_stopwatch.commit()
        except Exception as e:
            raise e
            logger.error(str(e))
            self.OUT_OF_SYNC = True
            return
        self.controller.consumer_list = self.controller.next_assignment
        self.controller.persist_consumer_state()
        self.controller.delete_consumers()
        self.controller.consumer_list.pretty_print()
        self.FINAL_GROUP_STATE = True


class StateGroupManagementTest(State): 
    """Dummy state to be used when testing the Approximation algorithms output
    for the different generated measurement sequences.

    """


    def __init__(self, controller):
        super().__init__(controller)
        self.FINAL_GROUP_STATE = None
        self.latencies = {}
        self.evaluation_metrics = [(
            "file", 
            "algorithm",
            "iteration",
            "Rscore_Consumer_Capacity", 
            "Rscore_Algorithm_Capacity", 
            "Number of Reassignments", 
            "Number of Consumers", 
        )]

    def entry(self): 
        super().entry()
        self.FINAL_GROUP_STATE = False

    def exit(self):
        super().exit()
        self.FINAL_GROUP_STATE = None

    def group_reached_state(self): 
        if self.FINAL_GROUP_STATE == None:
            raise InactiveState()
        return self.FINAL_GROUP_STATE

    def save_metrics(self, dir):
        with open(f"{dir}/algorithms_data", "w") as f: 
            csv.writer(f).writerows(self.evaluation_metrics)

        latencies = {}
        for (algo_name, _), values in self.latencies.items():
            algo_latencies = latencies.get(algo_name)
            if algo_latencies == None: 
                algo_latencies = []
                latencies[algo_name] = algo_latencies
            algo_latencies.extend(values)

        for algo_name, algo_latencies in latencies.items(): 
            with open(f"{dir}/latencies_{algo_name}", "w") as f:
                import json
                json.dump(algo_latencies, f)

    def execute(self): 
        delta = self.controller.next_assignment - self.controller.consumer_list
        reassigned_partitions = [p 
            for p, actions in delta.map_partition_actions.items() 
            if actions.stop != None
        ]
        Rscore_absolute = functools.reduce(
            lambda accum, p: p.speed + accum,
            reassigned_partitions, 0,
        )
        Nconsumers = len(self.controller.next_assignment.active_consumers)-1
        algorithm_name = self.controller.state_machine.states["reassign"].algorithm_name
        self.evaluation_metrics.append((
            self.controller.state_machine.states["sentinel"].file,
            algorithm_name,
            self.controller.state_machine.CYCLE,
            Rscore_absolute/CONSUMER_CAPACITY,
            Rscore_absolute/ALGO_CAPACITY,
            len(reassigned_partitions),
            Nconsumers
        ))

        # Change the order as start messages are not constructed before the 
        # stop message has been acknowledged. We can see which partitions would 
        # have to be stopped if the current assignment were switched with 
        # the next assignment
        delta = self.controller.consumer_list - self.controller.next_assignment
        for consumer in self.controller.next_assignment: 
            if consumer == None: 
                continue

            latency_key = (algorithm_name, consumer)
            latencies = self.latencies.get(latency_key)
            rebalance_latencies = []
            consumption_latencies = []
            if latencies == None:
                latencies = []
                self.latencies[latency_key] = latencies

            start_partitions = (
                delta.batch[consumer].stop.partitions() 
                if delta.batch.get(consumer) != None else set() 
            )
            rebalance_queue_wr = functools.reduce(
                lambda accum, tp: accum + tp.speed, 
                start_partitions, 0
            )
            consumption_queue_wr = consumer.combined_speed - rebalance_queue_wr
            consumption_queue_cr = (
                min(CONSUMER_CAPACITY, consumption_queue_wr) 
                if rebalance_queue_wr != 0 else CONSUMER_CAPACITY
            )
            rebalance_queue_cr = CONSUMER_CAPACITY - consumption_queue_cr

            # Consumption Latencies
            total_consumption_bytes = int(consumption_queue_wr*30)
            if total_consumption_bytes > 0:  
                m = 1/consumption_queue_cr - 1/consumption_queue_wr
                b = latencies[-1] if len(latencies) > 0 else 0
                for i in range(total_consumption_bytes):
                    latency = max(m*i + b, 0)
                    rebalance_latencies.append(latency)

            # Rebalance Latencies
            total_rebalance_bytes = (
                int(rebalance_queue_wr*30) 
                if self.controller.state_machine.CYCLE != 0 else 0
            )
            if total_rebalance_bytes > 0:
                m = 1/rebalance_queue_cr - 1/rebalance_queue_wr
                b = 5
                for i in range(total_rebalance_bytes):
                    latency = max(m*i + b, 0)
                    consumption_latencies.append(latency)

            latencies.extend(rebalance_latencies)
            latencies.extend(consumption_latencies)

        self.controller.consumer_list = self.controller.next_assignment
        self.FINAL_GROUP_STATE = True


class StateInitialize(State): 


    def __init__(self, controller):
        super().__init__(controller)
        self.HAS_PERSISTED_STATE = None

    def entry(self): 
        super().entry()

    def exit(self):
        super().exit()
        self.HAS_PERSISTED_STATE = None

    def persisted_state(self): 
        if self.HAS_PERSISTED_STATE == None:
            raise InactiveState()
        return self.HAS_PERSISTED_STATE

    def no_persisted_state(self): 
        if self.HAS_PERSISTED_STATE == None:
            raise InactiveState()
        return not self.HAS_PERSISTED_STATE

    def execute(self): 
        clist = self.controller.load_consumer_state()
        if clist == None: 
            self.HAS_PERSISTED_STATE = False 
        else:
            self.controller.consumer_list = ConsumerList.from_json(clist)
            self.controller.consumer_list.pretty_print()
            self.HAS_PERSISTED_STATE = True


class StateSynchronize(State): 


    def __init__(self, controller):
        super().__init__(controller)
        self.RECEIVED_QUERIES = None

    def entry(self): 
        super().entry()
        self.RECEIVED_QUERIES = False

    def exit(self):
        super().exit()
        self.RECEIVED_QUERIES = None

    def synchronized(self): 
        if self.RECEIVED_QUERIES == None:
            raise InactiveState()
        return self.RECEIVED_QUERIES

    def execute(self): 
        set_consumers = self.controller.active_consumers()
        self.controller.query_consumers(set_consumers)
        self.controller.wait_queries_response(set_consumers)
        self.controller.persist_consumer_state()
        self.controller.consumer_list.pretty_print()
        self.RECEIVED_QUERIES = True


class StateMachine:


    def __init__(self, controller, states=[], transitions=[]): 
        self.controller = controller
        self.initial = None
        self.current_state = None
        self.states = {key: st for key, st in states}
        self.CYCLE = 0
        for orig, dest, f in transitions: 
            sorig = self.states.get(orig)
            if sorig == None:
                raise InvaidStateKey(f"{sorig} was provided as key, but does not"
                                     "exist as a registered state")

            sdest = self.states.get(dest)
            if sdest == None:
                raise InvalidStateKey(f"{sdest} was provided as key, but does"
                                      "not exist as a registered state")

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
            raise InvaidStateKey(f"{sorig} was provided as key, but does not"
                                  "exist as a registered state")

        sdestination = self.state.get(destination_key)
        if sdestination == None:
            raise InvalidStateKey(f"{sdest} was provided as key, but does"
                                  "not exist as a registered state")

        t = Transition(function_transition, sorigin, sdestination)
        orig.add_transition(t)

    def set_initial(self, key):
        s = self.states.get(key)
        if s == None: 
            raise InvalidStateKey(f"{sdest} was provided as key, but does"
                                  "not exist as a registered state")
        self.initial = s

    def execute(self): 
        if self.current_state == None: 
            self.initialize()
        try: 
            self.current_state.execute()
        except SwitchAlgorithm: 
            self.CYCLE = 0
            self.controller.consumer_list = ConsumerList()
            self.current_state.execute()
            try: 
                self.states["reassign"].next_algorithm()
            except NextFile:
                try: 
                    self.states["sentinel"].next_file()
                except NoMoreFiles:
                    dir = "/usr/src/data"
                    self.states["manage"].save_metrics(dir)
                    logger.info(f"Metrics available in {dir}")
                    # time.sleep(60*5)
                    exit(0)

        sdest = self.current_state.verify_transitions()
        if sdest != None: 
            self.change_state(sdest)

    def change_state(self, destination):
        if (self.current_state == self.states["manage"]
            and destination == self.states["sentinel"]
        ): 
            self.CYCLE += 1
            print(f"Incrementing {self.CYCLE}")
            # self.controller.log_stopwatch.save_to_file("/usr/src/data/")
        self.current_state.exit()
        self.current_state = destination
        self.current_state.entry()


    def initialize(self): 
        if self.initial == None: 
            raise MissingInitialState()
        logger.debug("==== Starting State Machine ====")
        self.current_state = self.initial
        self.current_state.entry()

    @classmethod
    def factory_method(cls, controller, env): 
        if env == "test-controller": 
            return cls.create_test_controller_state_machine(controller)
        elif env == "test-algorithms": 
            return cls.create_test_algorithms_state_machine(controller)
        elif env == "test-step": 
            return cls.create_step_state_machine(controller)
        else: 
            return cls.create_controller_state_machine(controller)

    @classmethod
    def create_controller_state_machine(cls, controller):
        synchronize = StateSynchronize(controller)
        sentinel = StateSentinel(controller)
        reassign = StateReassignAlgorithm(controller, approximation_algorithm="mwf")
        manage = StateGroupManagement(controller)
        states = [
            ("synchronize", synchronize), 
            ("sentinel", sentinel), 
            ("reassign", reassign), 
            ("manage", manage), 
        ]
        transitions = [
            ("synchronize", "sentinel", synchronize.synchronized),
            ("sentinel", "reassign", sentinel.time_up),
            ("sentinel", "reassign", sentinel.full_bin),
            ("sentinel", "reassign", sentinel.any_unassigned),
            ("reassign", "manage", reassign.finished_approximation_algorithm),
            ("manage", "sentinel", manage.group_reached_state),
            ("manage", "synchronize", manage.out_of_sync),
        ]
        state_machine = cls(
            controller, states=states, transitions=transitions
        )
        state_machine.set_initial("synchronize")
        return state_machine

    @classmethod
    def create_test_controller_state_machine(cls, controller): 
        synchronize = StateSynchronize(controller)
        sentinel = StateSentinelFile(controller)
        reassign = StateReassignAlgorithmTest(controller)
        manage = StateGroupManagement(controller)
        states = [
            ("synchronize", synchronize), 
            ("sentinel", sentinel), 
            ("reassign", reassign), 
            ("manage", manage), 
        ]
        transitions = [
            ("synchronize", "sentinel", synchronize.synchronized),
            ("sentinel", "reassign", sentinel.time_up),
            ("sentinel", "reassign", sentinel.full_bin),
            ("sentinel", "reassign", sentinel.any_unassigned),
            ("reassign", "manage", reassign.finished_approximation_algorithm),
            ("manage", "sentinel", manage.group_reached_state),
            ("manage", "synchronize", manage.out_of_sync),
        ]
        state_machine = cls(
            controller, states=states, transitions=transitions
        )
        state_machine.set_initial("synchronize")
        return state_machine

    @classmethod
    def create_test_algorithms_state_machine(cls, controller): 
        sentinel = StateSentinelAlgorithms(controller)
        reassign = StateReassignAlgorithmTest(controller)
        manage = StateGroupManagementTest(controller)
        states = [
            ("sentinel", sentinel), 
            ("reassign", reassign), 
            ("manage", manage), 
        ]
        transitions = [
            ("sentinel", "reassign", sentinel.time_up),
            ("sentinel", "reassign", sentinel.full_bin),
            ("sentinel", "reassign", sentinel.any_unassigned),
            ("reassign", "manage", reassign.finished_approximation_algorithm),
            ("manage", "sentinel", manage.group_reached_state),
        ]
        state_machine = cls(
            controller, states=states, transitions=transitions
        )
        state_machine.set_initial("sentinel")
        return state_machine


    @classmethod
    def create_step_state_machine(cls, controller):
        synchronize = StateSynchronize(controller)
        sentinel = StateSentinelStep(controller)
        reassign = StateReassignAlgorithm(controller, approximation_algorithm="mwf")
        manage = StateGroupManagement(controller)
        states = [
            ("synchronize", synchronize), 
            ("sentinel", sentinel), 
            ("reassign", reassign), 
            ("manage", manage), 
        ]
        transitions = [
            ("synchronize", "sentinel", synchronize.synchronized),
            ("sentinel", "reassign", sentinel.time_up),
            ("sentinel", "reassign", sentinel.full_bin),
            ("sentinel", "reassign", sentinel.any_unassigned),
            ("reassign", "manage", reassign.finished_approximation_algorithm),
            ("manage", "sentinel", manage.group_reached_state),
            ("manage", "synchronize", manage.out_of_sync),
        ]
        state_machine = cls(
            controller, states=states, transitions=transitions
        )
        state_machine.set_initial("synchronize")
        return state_machine

