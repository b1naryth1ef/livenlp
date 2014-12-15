import sys

from multiprocessing import Process
from consumer import Consumer
from nlp_consumer import NLPConsumer
from reddit_consumer import RedditConsumer

q = Consumer.make_queue()
c = Consumer("twitter")

if len(sys.argv) > 1:
    c.consumer.seek(int(sys.argv[1]), 0)
else:
    c.consumer.seek(0, 2)

p = Process(target=c.run, args=(q, ))
p.start()

def reddit():
    reddit = RedditConsumer()
    while True:
        reddit.consume(q.get(True))

def nlp():
    nlp = NLPConsumer()
    while True:
        nlp.consume(q.get(True))

nlp()
