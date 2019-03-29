package parallel;

import org.apache.storm.Config;
import org.apache.storm.starter.tools.Rankings;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.TupleUtils;

import java.util.HashMap;
import java.util.Map;

public class LoggerPreparerParallelBolt extends BaseRichBolt {
    private OutputCollector collector;
    private long timestamp;
    private int emitFrequencyInSeconds;
    private final List<BucketEntry> aggregateRankings;

    public LoggerPreparerParallelBolt(int emitFrequencyInSeconds, int topN) {
        this.aggregateRankings = new ArrayList<BucketEntry>(topN);
        this.emitFrequencyInSeconds = emitFrequencyInSeconds;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        timestamp = System.currentTimeMillis();
    }


    @Override
    public void execute(Tuple tuple) {

		aggregateRankings.clear();
		for (BucketEntry e : ((List<BucketEntry>) tuple.getValue(0))) {
			aggregateRankings.add(e);
		}

		if (aggregateRankings.size() > 0) {
			collector.emit(new Values(timestamp, aggregateRankings));
			timestamp = System.currentTimeMillis();
		}
	}


    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("timestamp", "rankings"));
    }


    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> conf = new HashMap<>();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, emitFrequencyInSeconds);
        return conf;
    }
}
