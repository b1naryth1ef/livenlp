from producer import Parent
from twitter import TwitterFilterProducer, TwitterProducer

parent = Parent([TwitterProducer()])
parent.start()
raw_input()

