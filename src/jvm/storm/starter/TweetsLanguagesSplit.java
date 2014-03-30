package storm.starter;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.task.ShellBolt;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.tuple.Values;
import backtype.storm.tuple.Tuple;
import backtype.storm.task.TopologyContext;
import backtype.storm.task.OutputCollector;
import storm.starter.spout.TwitterSpout;
import storm.starter.bolt.RedisBolt;
import storm.starter.util.StormRunner;
import redis.clients.jedis.Jedis;
import java.util.HashMap;
import java.util.Map;
import java.io.InputStream;
import java.io.FileNotFoundException;
import java.io.FileInputStream;
import java.io.File;
import org.yaml.snakeyaml.Yaml;

/**
 * This topology identifies tweets' languages using LangId and Dysl
 */
public class TweetsLanguagesSplit {

  public static class Segmenter extends ShellBolt implements IRichBolt {

    public Segmenter() {
      super("python", "segmenter.py");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("id", "sents"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
      return null;
    }
  }

  public static class GetLanguageDysl extends ShellBolt implements IRichBolt {

    public GetLanguageDysl() {
      super("python", "getlanguage-dysl-split.py");
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

  public static class LanguageCount extends BaseRichBolt {
    Map<String, Integer> counts = new HashMap<String, Integer>();
    Map<String, Integer> ids = new HashMap<String, Integer>();
    String redishost;
    Integer redisport;
    OutputCollector collector;

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
      this.collector = collector;
      redishost = conf.get("redis_host").toString();
      redisport = ((Long) conf.get("redis_port")).intValue();
    }

    @Override
    public void execute(Tuple tuple) {
      String id = tuple.getString(0);
      Jedis jedis = new Jedis(redishost, redisport);
      if (ids.get(id) == null || jedis.get("tweet:" + id) == null) {
        ids.put(id, 1);
        String language = tuple.getString(1);
        Integer count = counts.get(language);
        if (count == null) {
          if (jedis.get("lang:" + language) == null) count = 0;
          else count = Integer.parseInt(jedis.get("lang:" + language));
        }
        count++;
        counts.put(language, count);
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

  public TweetsLanguagesSplit() throws InterruptedException {
    builder = new TopologyBuilder();
    topologyName = "TweetsLanguagesSplit";
    topologyConfig = createTopologyConfiguration();
    runtimeInSeconds = DEFAULT_RUNTIME_IN_SECONDS;

    wireTopology();
  }

  private static Config createTopologyConfiguration() {
    Config conf = new Config();
    conf.setDebug(true);

    // Other configurations come from YAML file
    FileInputStream input = null;
    try {
      input = new FileInputStream(new File("config.yml"));
    } catch(FileNotFoundException fnfe) {
      System.out.println(fnfe.getMessage());
    }
    Yaml yaml = new Yaml();
    Map<String, String> config = (Map<String, String>) yaml.load(input);
    
    conf.putAll(config);
    
    return conf;
  }

  private void wireTopology() throws InterruptedException {
    String spoutId = "twitterStream";
    String segmenter = "segmenter";
    String langDyslId = "langDyslId";
    String redisDyslId = "redisDysl";
    /*String langcountId = "languageCount";*/
    builder.setSpout(spoutId, new TwitterSpout(), 1);
    builder.setBolt(segmenter, new Segmenter(), 2).shuffleGrouping(spoutId);
    builder.setBolt(langDyslId, new GetLanguageDysl(), 2).shuffleGrouping(segmenter);
    builder.setBolt(redisDyslId, new RedisBolt(), 2).shuffleGrouping(langDyslId);
    /*builder.setBolt(langcountId, new LanguageCount(), 12).fieldsGrouping(langId, new Fields("language"));*/
  }

  public void run() throws Exception {
    String mode = topologyConfig.get("mode").toString();
    if (mode.equals("local")) {
      StormRunner.runTopologyLocally(builder.createTopology(), topologyName, topologyConfig, runtimeInSeconds);
    }
    else if (mode.equals("cluster")) {
      topologyConfig.setNumWorkers(3);
      StormSubmitter.submitTopology(topologyName, topologyConfig, builder.createTopology());
    }
    else {
      System.out.println("Unknown mode: " + mode);
    }
  }

  public static void main(String[] args) throws Exception {
    new TweetsLanguagesSplit().run();
  }
}
