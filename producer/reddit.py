import time, json, praw

from producer import Producer

class RedditProducer(Producer):
    TOPIC = "reddit"

    def __init__(self, *args, **kwargs):
        Producer.__init__(self, *args, **kwargs)
        self.client = praw.Reddit(user_agent="b1naryth1ef/livenlp")

    def format_single(self, post):
        keys = [k for k in post.__dict__ if not k.startswith("_")]
        data = dict([(k, post.__dict__[k]) for k in keys])

        for key, value in data.items():
            if type(value) not in [str, int, list, dict, unicode, float, long]:
                data[key] = str(value)
        return data

    def produce(self):
        last = None
        while True:
            if last:
                data = list(self.client.get_new(place_holder=last.id, limit=100))
            else:
                data = list(self.client.get_new(limit=100))

            if last and len(data) == 1 and last.id == data[0].id:
                time.sleep(1)
                continue
            print len(data)
            map(lambda i: self.queue.put(json.dumps(self.format_single(i)), False), data)
            last = data[0]
