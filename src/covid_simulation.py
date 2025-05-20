import random
import heapq
from fvg_logging import CsvLogger as Logger
from fvg_logging import JsonLogger
from datetime import datetime

# =============================================================================
# Events
# =============================================================================
# class: Event ----------------------------------------------------------------
class Event:
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
    TYPE = "ARRIVAL"
    def processEvent(self):
        if not self._simulation.enqueue_car(self._car_id): 
            self.log_self()
            return
        event = PreregistrationEvent(self._car_id,self._timestamp,self._person_count,self._simulation)
        self._simulation.add_event(event)
        self.log_self()

# class: PreregistrationEvent -------------------------------------------------
class PreregistrationEvent(Event):
    TYPE = "PREREG"
    def processEvent(self):
        timestamp = self._timestamp + random.randint(60,120)
        event = TestingEvent(self._car_id,timestamp,self._person_count,self._simulation)
        self._simulation.add_event(event)
        self.log_self()

# class: TestingEvent ---------------------------------------------------------
class TestingEvent(Event):
    TYPE = "TESTING"
    def processEvent(self):
        timestamp = self._timestamp + 240*self._person_count
        event = DepartureEvent(self._car_id,timestamp,self._person_count,self._simulation)
        self._simulation.add_event(event)
        self.log_self()

# class: DepartureEvent -------------------------------------------------------
class DepartureEvent (Event):
    TYPE = "DEPARTURE"
    def processEvent(self):
        self._simulation.dequeue_car(self._car_id)
        self.log_self()

# =============================================================================
# Simulation
# =============================================================================
class Simulation:
    MAX_CARS = 10
    MAX_ARRIVAL_TIME = 7200 # Zeit in Sekunden

    def __init__(self):
        self._event_queue = []
        self._car_queue = []
        self._id_counter = 0
        heapq.heapify(self._event_queue)
        # Logging
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        logname = f"simulation_log_{timestamp}"
        meta_logname = f"simulation_log_{timestamp}_meta"
        self._logger = Logger("time","car_id","person_count","event_type","cars_in_sys",filename = logname, path = "./logs")
        self._meta_logger = JsonLogger(filename = meta_logname, path = "./logs")
        self._meta_logger.log("queue_limit",str(self.MAX_CARS))
        self._meta_logger.log("time_limit",str(self.MAX_ARRIVAL_TIME))


        time = random.randint(30, 120)
        while time <= self.MAX_ARRIVAL_TIME:
            event = ArrivalEvent(self._id_counter,time,random.randint(1,5),self)
            self.add_event(event)
            step = random.randint(30, 120)
            time += step
            self._id_counter += 1

    def add_event(self, event):
        heapq.heappush(self._event_queue,event)
    def next_event(self):
        if not self._event_queue: return None
        return heapq.heappop(self._event_queue)
    def enqueue_car(self, car_id):
        if len(self._car_queue) >= self.MAX_CARS:
            return False
        self._car_queue.append(car_id)
        return True
    def dequeue_car(self, car_id):
        self._car_queue.remove(car_id)
    def car_count(self):
        return len(self._car_queue)
    
    def run(self):
        while( self._event_queue):
            event = self.next_event()
            event.processEvent()
    def log(self,*args) :
        self._logger.log(*args)


# =============================================================================
# Execution
# =============================================================================
simulation = Simulation()
simulation.run()