package kafka;
import StormBolt.CountBolt;
import StormBolt.MaxWord;
import StormBolt.WindowBolt;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.cassandra.bolt.CassandraWriterBolt;
import org.apache.storm.topology.base.BaseWindowedBolt;

import static org.apache.storm.cassandra.DynamicStatementBuilder.*;
public class KafkaStorm {
    public static void main(String[] args) throws Exception{
        Config config = new Config();
        config.setDebug(false);
        config.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
        config.put("cassandra.keyspace","output");
        config.put("cassandra.nodes","localhost");
        config.put("cassandra.port",9042);
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafka-spout", new KafkaSpout<String, String>(KafkaSpoutConfig.builder("localhost:9092", "jay").build()), 1);
        //builder.setBolt("word-spitter", new SplitBolt()).shuffleGrouping("kafka-spout");
        builder.setBolt("word-counter", new CountBolt()).shuffleGrouping("kafka-spout");
        //testing

        //removed for the windowsing puposes
        //builder.setBolt("max-counter", new MaxWord()).globalGrouping("word-counter");
        builder.setBolt("max-counter",new WindowBolt().withTumblingWindow(BaseWindowedBolt.Duration.of(6000))).globalGrouping("word-counter");;
        builder.setBolt("WORD_COUNT_CASSANDRA_BOLT", new CassandraWriterBolt(
                async(
                        simpleQuery("INSERT INTO output.maxxer  (key,value ) VALUES (?,?);")
                                .with(
                                        fields("key","value")
                                )
                )
        ),1).globalGrouping("max-counter");//globalGrouping("call-log-counter-bolt");

        LocalCluster cluster = new LocalCluster();
        //LocalCluster cluster = new LocalCluster("localhost",2181L);  //toexplicitly state the zookeeper
        cluster.submitTopology("KafkaStormSample", config, builder.createTopology());

        Thread.sleep(5000);
        //cluster.killTopology("KafkaStormSample"); not killing the topology so that it will run continuously
        //cluster.shutdown();
    }
}
