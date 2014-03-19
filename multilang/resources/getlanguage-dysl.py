import storm
from storm import BasicBolt
from dysl.langid import LangID

class GetLanguageDyslBolt(BasicBolt):

  def __init__(self, *args, **kwargs):
    #super(BasicBolt, self).__init__(*args, **kwargs)
    #print dir(BasicBolt)
    #BasicBolt.__init__(*args, **kwargs)
    self.l = LangID()
    self.l.train()

  def process(self, tup):
    text = tup.values[1]
    #language = langid.classify(text)[0]
    #l = LangID()
    #l.train()
    language = self.l.classify(text)
    storm.emit([tup.values[0], language])

GetLanguageDyslBolt().run()
