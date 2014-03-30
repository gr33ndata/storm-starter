import storm
from storm import BasicBolt
from dysl.langid import LangID

class GetLanguageDyslBolt(BasicBolt):

  def __init__(self, *args, **kwargs):
    self.l = LangID()
    self.l.train()

  def process(self, tup):
    sents = tup.values[1]
    languages = [self.l.classify(sent) for sent in sents]
    storm.emit([tup.values[0], '_'.join(languages)])

GetLanguageDyslBolt().run()
