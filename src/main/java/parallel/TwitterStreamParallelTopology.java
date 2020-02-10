package parallel;

import common.HashtagEmitterBolt;
import common.IndividualTagEmitterBolt;
import common.TwitterStreamSpout;
import local.*;
import org.apache.log4j.Logger;
import org.apache.storm.Config;
import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;
import org.apache.storm.starter.util.StormRunner;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class TwitterStreamParallelTopology {

    private static final Logger LOG = Logger.getLogger(TwitterStreamParallelTopology.class);

    private static final int DEFAULT_RUNTIME_IN_SECONDS = 120;
    private static final int TOP_N = 100;

    private final TopologyBuilder builder;
    private final String topologyName;
    private final Config topologyConfig;
    private final int runtimeInSeconds;

    public TwitterStreamParallelTopology(String topologyName) throws InterruptedException {
        builder = new TopologyBuilder();
        this.topologyName = topologyName;
        topologyConfig = createTopologyConfiguration();
        runtimeInSeconds = DEFAULT_RUNTIME_IN_SECONDS;
        buildRemoteTopology();
    }

    private static Config createTopologyConfiguration() {
        Config conf = new Config();
        conf.setDebug(true);
        conf.setNumWorkers(4);
        return conf;
    }

    private void buildRemoteTopology() throws InterruptedException {
        String spoutId = "tweetSpout";
        String hashtagsEmitterId = "hashtags";
        String individualTagsId = "singleTags";
        String tagCountId = "tagCounts";
        String interimRankerId = "interimRanker";
        String globalRankerId = "globalRanker";
        String tagLoggerId = "tagLogger";
        String hdfsId = "hdfsBolt";
        builder.setSpout(spoutId, new TwitterStreamSpout(), 1);
        builder.setBolt(hashtagsEmitterId, new HashtagEmitterBolt(), 3).fieldsGrouping(spoutId, new Fields("tweet"));
        builder.setBolt(individualTagsId, new IndividualTagEmitterBolt(), 3)
                .fieldsGrouping(hashtagsEmitterId, new Fields("hashtags"));
        builder.setBolt(tagCountId, new LossyCounterBolt(), 3).fieldsGrouping(individualTagsId, new Fields("tag"));
        builder.setBolt(interimRankerId, new RankingsBolt(), 3).fieldsGrouping(tagCountId, new Fields(
                "obj"));
        builder.setBolt(globalRankerId, new RankingsBolt(), 3)
                .globalGrouping(interimRankerId);
        builder.setBolt(tagLoggerId, new LoggerPreparerParallelBolt(TOP_N, 10), 3)
                .globalGrouping(globalRankerId);

        // sync the filesystem after every tuple
        SyncPolicy syncPolicy = new CountSyncPolicy(1);
        // rotate files when they reach 5MB
        FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(5.0f, FileSizeRotationPolicy.Units.MB);
        FileNameFormat fileNameFormat = new DefaultFileNameFormat().withPath("/rankings/");
        RecordFormat recordFormat = new DelimitedRecordFormat().withRecordDelimiter("\n").withFieldDelimiter(",");

        HdfsBolt hdfsBolt = new HdfsBolt().withFsUrl("hdfs://phoenix.cs.colostate.edu:30160")
                .withRecordFormat(recordFormat)
                .withFileNameFormat(fileNameFormat)
                .withSyncPolicy(syncPolicy)
                .withRotationPolicy(rotationPolicy);

        builder.setBolt(hdfsId, hdfsBolt, 1).globalGrouping(tagLoggerId);
    }

    public void runRemotely() throws Exception {
        StormRunner.runTopologyRemotely(builder.createTopology(), topologyName, topologyConfig);
    }

    /**
     * Submits (runs) the topology.
     *
     * Usage: "TwitterStreamParallelTopology [remote]"
     *
     * # Runs in remote/cluster mode, with topology name "production-topology"
     * $ storm jar pa2-1.0-SNAPSHOT.jar TwitterStreamTopology production-topology remote
     * ```
     *
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        String topologyName = "production-topology";

        LOG.info("Topology name: " + topologyName);
        TwitterStreamParallelTopology twitterStreamTopology = new TwitterStreamParallelTopology(topologyName);
        LOG.info("Running in remote (cluster) mode");
        twitterStreamTopology.runRemotely();
    }
}