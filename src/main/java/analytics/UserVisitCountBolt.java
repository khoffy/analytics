package analytics;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

public class UserVisitCountBolt extends BaseRichBolt {

    private HashMap<Integer, Integer> userVisitCounts;
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        userVisitCounts = new HashMap<>();
    }

    @Override
    public void execute(Tuple tuple) {
        Integer userId = tuple.getIntegerByField("userId");
        userVisitCounts.putIfAbsent(userId, 0);
        userVisitCounts.put(userId, userVisitCounts.get(userId) +1);
        System.out.printf("Received visit from user %d (total: %d)\n", userId, userVisitCounts.get(userId));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
