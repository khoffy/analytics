package analytics;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

public class UserVisitCountBolt extends BaseRichBolt {

    private HashMap<Integer, Integer> userVisitCounts;
    private OutputCollector outputCollector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        userVisitCounts = new HashMap<>();
        outputCollector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
        Integer userId = tuple.getIntegerByField("userId");
        userVisitCounts.putIfAbsent(userId, 0);
        userVisitCounts.put(userId, userVisitCounts.get(userId) +1);
        System.out.printf("Received visit from user %d (total: %d)\n", userId, userVisitCounts.get(userId));

        // Simulate a failure one out of 10
        if(ThreadLocalRandom.current().nextInt(10) == 0) {
            System.out.printf("--- Failed processing %s\n", tuple);
            // Direct fail() call in a Bolt
            outputCollector.fail(tuple);
        } else {
            outputCollector.ack(tuple);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
