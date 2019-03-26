import org.apache.log4j.Logger;
import org.apache.storm.Config;
import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;
import org.apache.storm.starter.bolt.IntermediateRankingsBolt;
import org.apache.storm.starter.util.StormRunner;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class TwitterStreamTopology {

    private static final Logger LOG = Logger.getLogger(TwitterStreamTopology.class);

    private static final int DEFAULT_RUNTIME_IN_SECONDS = 120;
    private static final int TOP_N = 100;

    private final TopologyBuilder builder;
    private final String topologyName;
    private final Config topologyConfig;
    private final int runtimeInSeconds;

    public TwitterStreamTopology(String topologyName) throws InterruptedException {
        builder = new TopologyBuilder();
        this.topologyName = topologyName;
        topologyConfig = createTopologyConfiguration();
        runtimeInSeconds = DEFAULT_RUNTIME_IN_SECONDS;

        buildTopology();
    }

    private static Config createTopologyConfiguration() {
        Config conf = new Config();
        conf.setDebug(true);
        return conf;
    }

    private void buildTopology() throws InterruptedException {
        String spoutId = "tweetSpout";
        String hashtagsEmitterId = "hashtags";
        String individualTagsId = "singleTags";
        String tagCountId = "tagCounts";
        String intermediateRankerId = "intermediateRanker";
        String tagLoggerId = "tagLogger";
        String hdfsId = "hdfsBolt";
        builder.setSpout(spoutId, new TwitterStreamSpout(), 1);
        builder.setBolt(hashtagsEmitterId, new HashtagEmitterBolt(), 3).fieldsGrouping(spoutId, new Fields("tweet"));
        builder.setBolt(individualTagsId, new IndividualTagEmitterBolt(), 3)
                .fieldsGrouping(hashtagsEmitterId, new Fields("hashtags"));
        builder.setBolt(tagCountId, new CountBolt(), 3).fieldsGrouping(individualTagsId, new Fields("tag"));
        builder.setBolt(intermediateRankerId, new IntermediateRankingsBolt(TOP_N, 10), 3).fieldsGrouping(tagCountId, new Fields(
                "obj"));
        builder.setBolt(tagLoggerId, new LoggerPreparerBolt(), 3).globalGrouping(intermediateRankerId);

        // sync the filesystem after every tuple
        SyncPolicy syncPolicy = new CountSyncPolicy(10);
        // rotate files when they reach 5MB
        FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(5.0f, FileSizeRotationPolicy.Units.MB);
        FileNameFormat fileNameFormat = new DefaultFileNameFormat()
                .withPath("/rankings/");

        HdfsBolt hdfsBolt = new HdfsBolt().withFsUrl("hdfs://phoenix.cs.colostate.edu:30160")
                .withFileNameFormat(fileNameFormat)
                .withSyncPolicy(syncPolicy)
                .withRotationPolicy(rotationPolicy);

        builder.setBolt(hdfsId, hdfsBolt, 1).shuffleGrouping(tagLoggerId);
    }

    public void runLocally() throws InterruptedException {
        StormRunner.runTopologyLocally(builder.createTopology(), topologyName, topologyConfig, runtimeInSeconds);
    }

    public void runRemotely() throws Exception {
        StormRunner.runTopologyRemotely(builder.createTopology(), topologyName, topologyConfig);
    }

    /**
     * Submits (runs) the topology.
     *
     * Usage: "TwitterStreamTopology [local|remote]"
     *
     * # Runs in local mode (LocalCluster), with topology name "tweetWindowCounts"
     * $ storm jar storm-starter-jar-with-dependencies.jar org.apache.storm.starter.RollingTopWords
     *
     * # Runs in remote/cluster mode, with topology name "production-topology"
     * $ storm jar storm-starter-jar-with-dependencies.jar org.apache.storm.starter.RollingTopWords production-topology remote
     * ```
     *
     * @param args First positional argument defines
     *             whether to run the topology locally ("local") or remotely, i.e. on a real cluster ("remote").
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        String topologyName = "tweetWindowCounts";

        boolean runLocally = true;
        if (args.length >= 2 && args[1].equalsIgnoreCase("remote")) {
            runLocally = false;
        }

        LOG.info("Topology name: " + topologyName);
        TwitterStreamTopology twitterStreamTopology = new TwitterStreamTopology(topologyName);
        if (runLocally) {
            LOG.info("Running in local mode");
            twitterStreamTopology.runLocally();
        }
        else {
            LOG.info("Running in remote (cluster) mode");
            twitterStreamTopology.runRemotely();
        }
    }
}