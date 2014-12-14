import time, json
from kafka import SimpleProducer, KafkaClient
from multiprocessing import Process

class Parent(object):
    def __init__(self, producers):
        self.kafka = KafkaClient("localhost:9092")
        self.producers = producers
        self.procs = map(lambda i: Process(target=i.run, args=(self.kafka, )), self.producers)

    def start(self):
        map(lambda i: i.start(), self.procs)

    def run(self):
        pass

class Producer(object):
    TOPIC = None

    def emit(self, obj):
        self.producer.send_messages(self.TOPIC, str(obj))

    def run(self, client):
        self.producer = SimpleProducer(client, async=True)
        while True:
            if self.can_produce():
                self.emit(self.produce())
            else:
                time.sleep(5)

    def can_produce(self):
        raise NotImplementedError("Producer.produce not implementted for %s" % self.__class__.__name__)

    def produce(self):
        raise NotImplementedError("Producer.produce not implementted for %s" % self.__class__.__name__)

