package parallel;

import org.apache.log4j.Logger;
import org.apache.storm.Config;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.TupleUtils;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class LossyCounterBolt extends BaseRichBolt {
    private static final Logger LOGGER = Logger.getLogger(LossyCounterBolt.class);
    private static final int DEFAULT_EMIT_FREQUENCY_IN_SECONDS = 10;
    private static final double DEFAULT_EPSILON_VALUE = 0.1;

    private final int itemsPerBucket;
    private final AtomicInteger bucketNumber = new AtomicInteger(1);
    private final AtomicInteger N = new AtomicInteger(0);
    private final HashMap<String, BucketEntry> bucketItems = new HashMap<>();

    private final int emitFrequency;
    private OutputCollector collector;

    public LossyCounterBolt() {
        this(DEFAULT_EMIT_FREQUENCY_IN_SECONDS, DEFAULT_EPSILON_VALUE);
    }

    public LossyCounterBolt(int emitFrequency, double epsilon) {
        this.emitFrequency = emitFrequency;
        this.itemsPerBucket = new BigDecimal(1/epsilon).setScale(0, RoundingMode.HALF_UP).intValue();
    }

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    public void execute(Tuple tuple) {
        if (TupleUtils.isTick(tuple)) {
            emitSortedRankings();
        } else {
            countObjAndAck(tuple);
            if (N.incrementAndGet() % itemsPerBucket == 0) {
                executeDeletePhase();
                bucketNumber.incrementAndGet();
            }
        }
    }

    //call when emitting (not necessarily after deletion phase because that happens on item counts vs. emitting on a timer).
    private void emitSortedRankings() {
        for (Map.Entry e : bucketItems.entrySet()) {
            collector.emit(new Values(e.getKey(), ((BucketEntry)e.getValue()).getCount()));
        }
    }

    private void countObjAndAck(Tuple tuple) {
        String tag = (String) tuple.getValue(0);

        bucketItems.merge(tag, new BucketEntry(tag, 1, this.getDelta()), (oldVal, newVal) -> {
            oldVal.incrementCount();
            return oldVal;
        });

        collector.ack(tuple);
    }

    private void executeDeletePhase() {
        synchronized (bucketItems) {
            bucketItems.entrySet().removeIf(e -> e.getValue().getFrequencyPlusDelta() <= this.bucketNumber.get());
        }
    }

    private int getDelta() {
        return bucketNumber.get() - 1;
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
