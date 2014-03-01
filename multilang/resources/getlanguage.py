import storm
import langid

class GetLanguageBolt(storm.BasicBolt):
  def process(self, tup):
    text = tup.values[1]
    language = langid.classify(text)[0]
    storm.emit([tup.values[0], language])

GetLanguageBolt().run()
