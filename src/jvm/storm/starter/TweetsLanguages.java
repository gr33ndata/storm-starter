package storm.starter;

import backtype.storm.Config;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.task.ShellBolt;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.tuple.Values;
import backtype.storm.tuple.Tuple;
import storm.starter.spout.TwitterSpout;
import storm.starter.bolt.RedisBolt;
import storm.starter.util.StormRunner;
import redis.clients.jedis.Jedis;
import java.util.HashMap;
import java.util.Map;

/**
 * This topology identifies tweets' languages using LangId
 */
public class TweetsLanguages {

  public static class GetLanguage extends ShellBolt implements IRichBolt {

    public GetLanguage() {
      super("python", "getlanguage.py");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("id", "language"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
      return null;
    }
  }

  public static class LanguageCount extends BaseBasicBolt {
    Map<String, Integer> counts = new HashMap<String, Integer>();
    Map<String, Integer> ids = new HashMap<String, Integer>();

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
      String id = tuple.getString(0);
      if (ids.get(id) == null) {
        ids.put(id, 1);
        String language = tuple.getString(1);
        Integer count = counts.get(language);
        if (count == null)
          count = 0;
        count++;
        counts.put(language, count);
        Jedis jedis = new Jedis("localhost");
        jedis.set("lang:" + language, String.valueOf(count));
        collector.emit(new Values(language, count));
      }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("language", "count"));
    }
  }

  private static final int DEFAULT_RUNTIME_IN_SECONDS = 300;

  private final TopologyBuilder builder;
  private final String topologyName;
  private final Config topologyConfig;
  private final int runtimeInSeconds;

  public TweetsLanguages() throws InterruptedException {
    builder = new TopologyBuilder();
    topologyName = "getLanguages";
    topologyConfig = createTopologyConfiguration();
    runtimeInSeconds = DEFAULT_RUNTIME_IN_SECONDS;

    wireTopology();
  }

  private static Config createTopologyConfiguration() {
    Config conf = new Config();
    conf.setDebug(true);
    return conf;
  }

  private void wireTopology() throws InterruptedException {
    String spoutId = "twitterStream";
    String langId = "langId";
    String redisId = "redis";
    String langcountId = "languageCount";
    builder.setSpout(spoutId, new TwitterSpout(), 3);
    builder.setBolt(langId, new GetLanguage(), 5).shuffleGrouping(spoutId);
    builder.setBolt(redisId, new RedisBolt(), 8).shuffleGrouping(langId);
    builder.setBolt(langcountId, new LanguageCount(), 12).fieldsGrouping(langId, new Fields("language"));
  }

  public void run() throws InterruptedException {
    StormRunner.runTopologyLocally(builder.createTopology(), topologyName, topologyConfig, runtimeInSeconds);
  }

  public static void main(String[] args) throws Exception {
    new TweetsLanguages().run();
  }
}
