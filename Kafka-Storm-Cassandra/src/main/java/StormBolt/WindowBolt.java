package StormBolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;

import java.sql.Timestamp;
import java.util.*;

public class WindowBolt extends BaseWindowedBolt {
    private OutputCollector collector;
    Map<String, Integer> maxi;
    int maxValue;
    Date date;
    long time;
    Timestamp ts;

    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        maxi = new HashMap<String, Integer>();
        maxValue=0;
        date= new Date();
        time= date.getTime();
        ts= new Timestamp(time);
    }


    public void execute(TupleWindow inputWindow) {
        int sum = 0;
        List<Tuple> tuplesInWindow = inputWindow.get();
        //LOG.debug("Events in current window: " + tuplesInWindow.size());
        if (tuplesInWindow.size() > 0) {
            /*
             * Since this is a tumbling window calculation,
             * we use all the tuples in the window to compute the avg.
             */
            for (Tuple tuple : tuplesInWindow) {
                String str = tuple.getStringByField("key").toString();

                int val = Integer.parseInt(tuple.getStringByField("value"));
                //System.out.println("Outterxxxxxxxxxxxxxxxxx: "+str +"-----"+val+"<><>"+maxValue+"--Counters: "+maxi.size());
                if(maxValue<val){
                    maxValue=val;
                    maxi.clear();
                    maxi.put(str,val);
                }
                else if(maxValue==val){
                    maxi.put(str,val);
                }


            }
            Iterator it = maxi.keySet().iterator();       //keyset is a method
            int counter=0;
            while(it.hasNext() && counter<=10)
            {
                String key=it.next().toString();
                String value = maxi.get(key).toString();
                //System.out.println("Maximum appearead word: "+key+"     & number: "+value);
                collector.emit(new Values(ts.toString(),key, value));
                counter++;
            }

        }
    }


    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("timestamp","key", "value"));
    }
}

