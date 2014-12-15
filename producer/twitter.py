import time, json
from producer import Producer

from multiprocessing import Queue

from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream

from config import twitter

class TwitterListener(StreamListener):
    def __init__(self, queue, *args, **kwargs):
        StreamListener.__init__(self, *args, **kwargs)
        self.queue = queue

    def on_data(self, data):
        self.queue.put(data, False)
        return True

    def on_error(self, status):
        print status

class TwitterProducer(Producer):
    TOPIC = "twitter"

    def __init__(self, *args, **kwargs):
        Producer.__init__(self, *args, **kwargs)
        self.auth = OAuthHandler(twitter.get("ckey"), twitter.get("csecret"))
        self.auth.set_access_token(twitter.get("atoken"), twitter.get("asecret"))
        self.li = TwitterListener(self.queue)
        self.stream = Stream(self.auth, self.li)

    def produce(self):
        self.stream.sample(languages=["en"])

class TwitterFilterProducer(TwitterProducer):
    def __init__(self, keywords, *args, **kwargs):
        TwitterProducer.__init__(self, *args, **kwargs)
        self.keywords = keywords

    def produce(self):
        self.stream.filter(track=self.keywords, languages=["en"])

