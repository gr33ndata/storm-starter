import storm
from storm import BasicBolt
from nltk import tokenize

class SegmenterBolt(BasicBolt):

  def __init__(self, *args, **kwargs):
    pass

  def process(self, tup):
    text = tup.values[1]
    sents = tokenize.sent_tokenize(text)
    storm.emit([tup.values[0], sents])

SegmenterBolt().run()