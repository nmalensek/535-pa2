import org.apache.storm.starter.tools.Rankings;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class LoggerPreparerBolt extends BaseRichBolt {
    private OutputCollector collector;
    private long timestamp;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        timestamp = System.currentTimeMillis();
    }

    @Override
    public void execute(Tuple tuple) {
        Rankings rankings = ((Rankings) tuple.getValue(0));
        rankings.pruneZeroCounts();
        System.out.println("*************************************************\n" +
                "*************************************************\n" +
                "*************************************************\n" +
                "*************************************************\n" +
                "*************************************************\n");
        collector.emit(new Values(timestamp, rankings));
        timestamp = System.currentTimeMillis();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("timestamp", "rankings"));
    }
}
