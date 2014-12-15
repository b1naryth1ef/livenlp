from producer import Parent
from twitter import TwitterFilterProducer, TwitterProducer
from reddit import RedditProducer

parent = Parent([RedditProducer(), TwitterProducer()])
parent.start()
raw_input()

