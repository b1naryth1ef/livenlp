import time, json
from kafka import SimpleProducer, KafkaClient
from multiprocessing import Process, Queue

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

    def __init__(self, senders=2):
        self.queue = Queue(4096)
        self.senders = senders

    def send_loop(self):
        kafka = KafkaClient("localhost:9092")
        producer = SimpleProducer(kafka, async=True)

        while True:
            producer.send_messages(self.TOPIC, str(self.queue.get(True)))

    def run(self, client):
        self.senders = map(lambda i: Process(target=self.send_loop), range(self.senders))
        self.process = Process(target=self.produce)
        self.process.start()
        map(lambda i: i.start(), self.senders)

    def produce(self):
        raise NotImplementedError("Producer.produce not implemented for %s" % self.__class__.__name__)

