package storm.starter.bolt;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import redis.clients.jedis.Jedis;

public class RedisBolt extends BaseBasicBolt {

  @Override
  public void execute(Tuple tuple, BasicOutputCollector collector) {
    String id = tuple.getString(0);
    String language = tuple.getString(1);
    Jedis jedis = new Jedis("localhost");
    jedis.set(id, language);
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
  }

}
