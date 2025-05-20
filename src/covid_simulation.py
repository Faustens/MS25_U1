# Version: Python 3.10.0

import random
import heapq
from fvg_logging import CsvLogger as Logger
from fvg_logging import JsonLogger
from datetime import datetime

# =============================================================================
# Events
# -----------------------------------------------------------------------------
# Events consist of a TYPE, an init method setting insance variables,
#   comparator methods which order events based on timestamp, a
#   processEvent-method and a method log_self
# =============================================================================
# class: Event ----------------------------------------------------------------
class Event:
    """
    Base event type
    Parameters: 
        car_id: ID of the event's car
        timestamp: The simulated time in seconds when the event took place
        person_count: Amount of people in a given car
        simulation: The simulation object the event is to be associated with
    """
    TYPE = "BASE_EVENT"
    def __init__(self,car_id,timestamp,person_count,simulation):
        self._car_id = car_id
        self._timestamp = timestamp
        self._person_count = person_count
        self._simulation = simulation
    def __eq__(self,other):
        return self._timestamp == other._timestamp
    def __ne__(self,other):
        return self._timestamp != other._timestamp
    def __lt__(self,other):
        return self._timestamp < other._timestamp
    def __le__(self,other):
        return self._timestamp <= other._timestamp
    def __gt__(self,other):
        return self._timestamp > other._timestamp
    def __ge__(self,other):
        return self._timestamp >= other._timestamp
    def log_self(self):
        self._simulation.log(self._timestamp,self._car_id,self._person_count,self.TYPE,self._simulation.car_count())

    def processEvent(self):
        pass

# class: ArrivalEvent ---------------------------------------------------------
class ArrivalEvent (Event):
    """
    The ArrivalEvent initiates an event chain, by first testing if the queue is full and
     adding a Preregistration event if there is still room. If yes, the car will be
     enqueued
    """
    TYPE = "ARRIVAL"
    def processEvent(self):
        self.log_self()
        if not self._simulation.enqueue_car(self._car_id): 
            return
        event = PreregistrationEvent(self._car_id,self._timestamp,self._person_count,self._simulation)
        self._simulation.add_event(event)

# class: PreregistrationEvent -------------------------------------------------
class PreregistrationEvent (Event):
    """
    A PregistrationEvent happens immediately after an ArrivalEvent iff. the queue had room for
     the associated car. It determines a random time between 1-2 minutes and creates a
     TestingEvent at that timestamp
    """
    TYPE = "PREREG"
    def processEvent(self):
        timestamp = self._timestamp + random.randint(60,120)
        event = TestingEvent(self._car_id,timestamp,self._person_count,self._simulation)
        self._simulation.add_event(event)
        self.log_self()

# class: TestingEvent ---------------------------------------------------------
class TestingEvent (Event):
    """
    The TestingEvent calulates a timestamp 4 minutes per person in the future and creates
     a Departure event for that time
    """
    TYPE = "TESTING"
    def processEvent(self):
        timestamp = self._timestamp + 240*self._person_count
        event = DepartureEvent(self._car_id,timestamp,self._person_count,self._simulation)
        self._simulation.add_event(event)
        self.log_self()

# class: DepartureEvent -------------------------------------------------------
class DepartureEvent (Event):
    """
    The last event in an event-chain. Removes the car from the simulator's car-queue
    """
    TYPE = "DEPARTURE"
    def processEvent(self):
        self._simulation.dequeue_car(self._car_id)
        self.log_self()

# =============================================================================
# Simulation
# =============================================================================
class Simulation:
    """
    The Object responsible for setting up a Covid-simulation, populating the event
     queue with the first events and iterating over the queue to process all events.
     Also responsible for providing a Logger for all Events to shar.
    """
    MAX_CARS = 10
    MAX_ARRIVAL_TIME = 7200 # Zeit in Sekunden

    def __init__(self):
        # Set up event and car-queue and transforming the former into a heap (priority queue)
        self._event_queue = []
        self._car_queue = []
        self._id_counter = 0
        heapq.heapify(self._event_queue)
        # Set up logging and write a meta-data-log
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        logname = f"simulation_log_{timestamp}"
        meta_logname = f"simulation_log_{timestamp}_meta"
        self._logger = Logger("time","car_id","person_count","event_type","cars_in_sys",filename = logname, path = "./logs")
        self._meta_logger = JsonLogger(filename = meta_logname, path = "./logs")
        self._meta_logger.log("queue_limit",str(self.MAX_CARS))
        self._meta_logger.log("time_limit",str(self.MAX_ARRIVAL_TIME))
        # Populate the event queue with ArrivalEvents ever 30 to 120 minutes
        time = random.randint(30, 120)
        while time <= self.MAX_ARRIVAL_TIME:
            event = ArrivalEvent(self._id_counter,time,random.randint(1,5),self)
            self.add_event(event)
            step = random.randint(30, 120)
            time += step
            self._id_counter += 1
    # Event management ----------------------------------------------
    def add_event(self, event):
        heapq.heappush(self._event_queue,event)
    def next_event(self):
        if not self._event_queue: return None
        return heapq.heappop(self._event_queue)
    # Car Queue management ------------------------------------------
    def enqueue_car(self, car_id):
        """
        Tries to add a new car to the car_queue.
         Returns False if the queue is full
        """
        if len(self._car_queue) >= self.MAX_CARS:
            return False
        self._car_queue.append(car_id)
        return True
    def dequeue_car(self, car_id):
        self._car_queue.remove(car_id)
    def car_count(self):
        return len(self._car_queue)
    # Run-method ----------------------------------------------------
    def run(self):
        """
        Iterates over the event-queue, calling each Event's processEvent-method
        """
        while(self._event_queue):
            event = self.next_event()
            event.processEvent()
    # Logging interface ---------------------------------------------
    def log(self,*args) :
        """Logging interface for Event-Instances to use"""
        self._logger.log(*args)

# =============================================================================
# Execution
# -----------------------------------------------------------------------------
# Because of the way the logging is currently written, the program has to be 
#  called from the project root to create the logging files under the correct
#  directory.
# =============================================================================
simulation = Simulation()
simulation.run()