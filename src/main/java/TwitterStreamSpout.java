import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import twitter4j.*;
import twitter4j.auth.AccessToken;
import twitter4j.conf.ConfigurationBuilder;
import org.apache.log4j.Logger;

import java.util.LinkedList;
import java.util.Map;

public class TwitterStreamSpout extends BaseRichSpout {
    public static Logger LOG = org.apache.log4j.Logger.getLogger(TwitterStreamSpout.class);
    boolean isDistributed;
    SpoutOutputCollector collector;
    private LinkedList<Status> tweetList = new LinkedList<>();

    public TwitterStreamSpout() {
        this(true);
    }

    public TwitterStreamSpout(boolean isDistributed) {
        this.isDistributed = isDistributed;
    }

    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;

        TwitterStream twitterStream = new TwitterStreamFactory(
                new ConfigurationBuilder().setJSONStoreEnabled(true).build()).getInstance();

            twitterStream.setOAuthConsumer("",
                    "");
        AccessToken token = new AccessToken("",
                "");
            twitterStream.setOAuthAccessToken(token);

                twitterStream.addListener(new StatusListener() {
                    public void onStatus(Status status) {
                        tweetList.add(status);
                    }

                    public void onStallWarning(StallWarning warning) {
                        LOG.info("Got warning: " + warning);
                    }

                    public void onException(Exception ex) {
                        ex.printStackTrace();
                    }

                    public void onTrackLimitationNotice(int numberOfLimitedStatuses) {}
                    public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {}
                    public void onScrubGeo(long userId, long upToStatusId) {}
                }).sample();
    }

    public void nextTuple() {
        Status tweet = tweetList.poll();
        if (tweet != null) {
            collector.emit(new Values(tweet));
        } else {
            Utils.sleep(10);
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("tweet"));
    }
}
