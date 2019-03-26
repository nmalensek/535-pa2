import org.apache.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import twitter4j.HashtagEntity;
import twitter4j.Status;

import java.util.Arrays;
import java.util.Map;

public class HashtagEmitterBolt extends BaseRichBolt {
    private static final Logger LOGGER = Logger.getLogger(HashtagEmitterBolt.class);
    private OutputCollector collector;

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    public void execute(Tuple tuple) {
        String[] tagArray = Arrays.stream(((Status) tuple.getValue(0)).getHashtagEntities()).map(HashtagEntity::getText).toArray(String[]::new);
        if (tagArray.length > 0) {
            collector.emit(new Values(String.join(",", tagArray)));
        }
        collector.ack(tuple);
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("hashtags"));
    }
}
