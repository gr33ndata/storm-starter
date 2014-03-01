package storm.starter.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.io.InputStream;
import java.io.FileNotFoundException;
import java.io.FileInputStream;
import java.io.File;

import twitter4j.*;
import twitter4j.FilterQuery;
import twitter4j.conf.ConfigurationBuilder;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;

import org.yaml.snakeyaml.Yaml;

public class TwitterSpout extends BaseRichSpout {
  SpoutOutputCollector _collector;
  LinkedBlockingQueue<Status> queue = null;
  TwitterStream _twitterStream;

  @Override
  public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
    _collector = collector;
    queue = new LinkedBlockingQueue<Status>(1000);

    // Read configuration
    FileInputStream input = null;
    try {
      input = new FileInputStream(new File("config.yml"));
    } catch(FileNotFoundException fnfe) {
      System.out.println(fnfe.getMessage());
    }
    Yaml yaml = new Yaml();
    Map<String, String> config = (Map<String, String>) yaml.load(input);

    ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();
    configurationBuilder.setOAuthConsumerKey(config.get("twitter_oauth_consumer_key"))
      .setOAuthConsumerSecret(config.get("twitter_oauth_consumer_secret"))
      .setOAuthAccessToken(config.get("twitter_oauth_access_token"))
      .setOAuthAccessTokenSecret(config.get("twitter_oauth_access_token_secret"));
    StatusListener listener = new StatusListener() {
      @Override
        public void onStatus(Status status) {
          queue.offer(status);
        }
      @Override
        public void onDeletionNotice(StatusDeletionNotice sdn) {
        }
      @Override
        public void onTrackLimitationNotice(int i) {
        }
      @Override
        public void onScrubGeo(long l, long l1) {
        }
      @Override
        public void onException(Exception e) {
        }
      @Override
        public void onStallWarning(StallWarning warning) {
        }
    };
    TwitterStreamFactory fact = new TwitterStreamFactory(configurationBuilder.build());
    _twitterStream = fact.getInstance();
    _twitterStream.addListener(listener);
    String keywords[] = { config.get("twitter_keyword") };
    _twitterStream.filter(new FilterQuery().track(keywords));
  }

  @Override
  public void nextTuple() {
    Status ret = queue.poll();
    if (ret == null) {
      Utils.sleep(50);
    } else {
      String sentence = ret.getText();
      String id = String.valueOf(ret.getId());
      _collector.emit(new Values(id, sentence));
    }
  }

  @Override
  public void ack(Object id) {
  }

  @Override
  public void fail(Object id) {
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("id", "text"));
  }

}
