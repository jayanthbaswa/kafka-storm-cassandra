package StormBolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class MaxWord implements IRichBolt {
    Map<String, Integer> counters;
    private OutputCollector collector;
    int maxValue;
    public void prepare(Map stormConf, TopologyContext context,
                        OutputCollector collector) {
        this.counters = new HashMap<String, Integer>();
        this.collector = collector;
        this.maxValue=0;
    }

    public void execute(Tuple input) {
        String str = input.getStringByField("key").toString();

        int val = Integer.parseInt(input.getStringByField("value"));
        System.out.println("Outterxxxxxxxxxxxxxxxxx: "+str +"-----"+val+"<><>"+maxValue+"--Counters: "+counters.size());
        if(maxValue<val){
           maxValue=val;
            counters.clear();
            counters.put(str,val);
        }
        else if(maxValue==val){
            counters.put(str,val);
        }
        Iterator it = counters.keySet().iterator();       //keyset is a method
        while(it.hasNext())
        {
            String key=it.next().toString();
            String value = counters.get(key).toString();
            System.out.println("Roll no.: "+key+"     name: "+value);
            collector.emit(new Values(str, value));
        }
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