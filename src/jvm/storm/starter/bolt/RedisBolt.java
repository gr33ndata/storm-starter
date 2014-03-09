package storm.starter.bolt;

import backtype.storm.task.TopologyContext;
import backtype.storm.task.OutputCollector;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import java.util.Map;
import redis.clients.jedis.Jedis;

public class RedisBolt extends BaseRichBolt {
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
    String language = tuple.getString(1);
    Jedis jedis = new Jedis(redishost, redisport);
    jedis.set("tweet:" + id, language);
    collector.emit(new Values(id, language));
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("id", "language"));
  }

}
