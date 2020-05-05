package StormBolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class SplitBolt implements IRichBolt {
    private OutputCollector outputCollector;

    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
     this.outputCollector=outputCollector;
    }

    public void execute(Tuple tuple) {
        String str = tuple.getString(4);
        outputCollector.emit(new Values(str));
        outputCollector.ack(tuple);

    }


    public void cleanup() {

    }


    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    outputFieldsDeclarer.declare(new Fields("words"));
    }


    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
