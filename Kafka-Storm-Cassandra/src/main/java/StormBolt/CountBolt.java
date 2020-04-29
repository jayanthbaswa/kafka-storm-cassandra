package StormBolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

public class CountBolt implements IRichBolt {
    Map<String, Integer> counters;
    private OutputCollector collector;

    public void prepare(Map stormConf, TopologyContext context,
                        OutputCollector collector) {
        this.counters = new HashMap<String, Integer>();
        this.collector = collector;
    }

    public void execute(Tuple input) {
        String str = input.getString(4).toString();

        Integer c=0;
        if(!counters.containsKey(str)){
            counters.put(str, 1);
        }else {
            c = counters.get(str) +1;
            counters.put(str, c);
        }
        collector.emit(new Values(str, counters.get(str).toString()));
        collector.ack(input);
    }

    public void cleanup() {
        for(Map.Entry<String, Integer> entry:counters.entrySet()){
            System.out.println(entry.getKey()+" : " + entry.getValue());
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("key", "value"));
    }


    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
