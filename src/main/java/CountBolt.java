import org.apache.log4j.Logger;
import org.apache.storm.Config;
import org.apache.storm.starter.tools.SlidingWindowCounter;
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

public class CountBolt extends BaseRichBolt {
    private static final Logger LOGGER = Logger.getLogger(CountBolt.class);
    private static final int DEFAULT_WINDOW_SIZE_IN_SECONDS = 60;
    private static final int DEFAULT_EMIT_FREQUENCY_IN_SECONDS = 10;

    private final int windowSize;
    private final int emitFrequency;
    private final SlidingWindowCounter<Object> counter;
    private OutputCollector collector;

    public CountBolt() {
        this(DEFAULT_WINDOW_SIZE_IN_SECONDS, DEFAULT_EMIT_FREQUENCY_IN_SECONDS);
    }

    public CountBolt(int windowSize, int emitFrequency) {
        this.windowSize = windowSize;
        this.emitFrequency = emitFrequency;
        counter = new SlidingWindowCounter<>(deriveNumWindowChunksFrom(this.windowSize,
                this.emitFrequency));
    }

    private int deriveNumWindowChunksFrom(int windowLengthInSeconds, int windowUpdateFrequencyInSeconds) {
        return windowLengthInSeconds / windowUpdateFrequencyInSeconds;
    }

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    public void execute(Tuple tuple) {
        if (TupleUtils.isTick(tuple)) {
            LOGGER.debug("Received tick tuple, triggering emit of current window counts");
            emitCurrentWindowCounts();
        } else {
            countObjAndAck(tuple);
        }
    }

    private void emitCurrentWindowCounts() {
        Map<Object, Long> counts = counter.getCountsThenAdvanceWindow();
        emit(counts);
    }

    private void emit(Map<Object, Long> counts) {
        for (Map.Entry<Object, Long> entry : counts.entrySet()) {
            Object obj = entry.getKey();
            Long count = entry.getValue();
            collector.emit(new Values(obj, count));
        }
    }

    private void countObjAndAck(Tuple tuple) {
        Object obj = tuple.getValue(0);
        counter.incrementCount(obj);
        collector.ack(tuple);
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> conf = new HashMap<>();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, emitFrequency);
        return conf;
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("obj", "count"));
    }
}
