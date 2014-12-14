import time, json
from producer import Producer

from Queue import Queue

from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream

from config import twitter

class TwitterListener(StreamListener):
    q = Queue(4096)

    def on_data(self, data):
        self.q.put(data, False)
        return True

    def on_error(self, status):
        print status

class TwitterProducer(Producer):
    TOPIC = "twitter"

    def __init__(self, *args, **kwargs):
        Producer.__init__(self, *args, **kwargs)
        self.auth = OAuthHandler(twitter.get("ckey"), twitter.get("csecret"))
        self.auth.set_access_token(twitter.get("atoken"), twitter.get("asecret"))
        self.li = TwitterListener()
        self.stream = Stream(self.auth, self.li)

    def run(self, client):
        self.stream.sample(True, languages=["en"])
        Producer.run(self, client)

    def can_produce(self):
        return bool(self.li.q.qsize())

    def produce(self):
        return self.li.q.get(True, 60)

class TwitterFilterProducer(TwitterProducer):
    def __init__(self, keywords, *args, **kwargs):
        TwitterProducer.__init__(self, *args, **kwargs)
        self.keywords = keywords

    def run(self, client):
        self.stream.filter(track=self.keywords, languages=["en"], async=True)
        Producer.run(self, client)

