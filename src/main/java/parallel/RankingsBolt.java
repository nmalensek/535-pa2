package parallel;

import org.apache.storm.Config;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.TupleUtils;

import java.util.*;

public class RankingsBolt extends BaseRichBolt {
    private static final int TOP_N = 100;
    private static final int DEFAULT_EMIT_FREQUENCY_IN_SECONDS = 10;

    private OutputCollector collector;
    private final HashMap<String, BucketEntry> entries = new HashMap<>();

    public RankingsBolt() { }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        if (TupleUtils.isTick(tuple)) {
            List<BucketEntry> rankingsCopy;
            synchronized (entries) {
                rankingsCopy = new ArrayList<>(entries.values());
            }
            Collections.sort(rankingsCopy);
            Collections.reverse(rankingsCopy);

            List<BucketEntry> anotherCopy = new ArrayList<>();
            for (int i = 0; i < TOP_N; i++) {
                anotherCopy.add(rankingsCopy.get(i));
            }
            collector.emit(new Values(anotherCopy));
        } else {
            List<BucketEntry> newEntries = (List<BucketEntry>) tuple;
            for (BucketEntry e : newEntries) {
                entries.merge(e.getHashtag(), e, (oldVal, newVal) -> {
                    int oldCount = oldVal.getCount();
                    int newCount = newVal.getCount();
                    oldVal.setCount(oldCount + newCount);
                    return oldVal;
                });
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("rankings"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> conf = new HashMap<>();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, DEFAULT_EMIT_FREQUENCY_IN_SECONDS);
        return conf;
    }
}
