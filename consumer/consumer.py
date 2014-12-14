import json
from kafka import KafkaClient, SimpleConsumer
from multiprocessing import Queue

class Consumer(object):
    def __init__(self, topic):
        self.kafka = KafkaClient("localhost:9092")
        self.consumer = SimpleConsumer(self.kafka, "1", topic)

    @classmethod
    def make_queue(cls):
        return Queue(4096)

    def run(self, q):
        try:
            for i in self.consumer:
                q.put(json.loads(i.message.value), True)
        except Exception as e:
            self.kafka.close()

