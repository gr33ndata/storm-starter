import storm
#import langid
from dysl.langid import LangID

class GetLanguageBolt(storm.BasicBolt):
  def process(self, tup):
    text = tup.values[1]
    #language = langid.classify(text)[0]
    l = LangID()
    l.train()
    language = l.classify(text)
    storm.emit([tup.values[0], language])

GetLanguageBolt().run()
