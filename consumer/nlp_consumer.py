from textblob import TextBlob
from influxdb import InfluxDBClient
import string
import datetime

client = InfluxDBClient('localhost', 8086, 'root', 'root', 'nlp')

class NLPConsumer(object):
    def consume(self, obj):
        if obj["in_reply_to_screen_name"]: return
        if obj['text'].startswith("RT "): return

        blob = TextBlob(obj['text'].lower())
        time = datetime.datetime.strptime(obj['created_at'], "%a %b %d %H:%M:%S +0000 %Y")

        points = []
        for entry in blob.tags:
            word = entry[0]
            if word in string.punctuation: continue
            if len(word) == 1: continue
            points.append(word)

        whence = int((time - datetime.datetime(1970,1,1)).total_seconds())
        client.write_points([{
            "name": "word",
            "columns": ["time", "word"],
            "points": [
                [whence, word] for word in points
            ]
        }])

        client.write_points([{
            "name": "tweets",
            "columns": ["time", "screen_name", "followers_count", "polarity"],
            "points": [
                [whence, obj["user"]["screen_name"], obj["user"]["followers_count"], blob.sentiment.polarity]
            ]
        }])
