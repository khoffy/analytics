package analytics;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

public class PageVisitCountBolt extends BaseRichBolt {

    private HashMap<String, Integer> pageVisitCounts;
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        // Initialisation
        pageVisitCounts = new HashMap<>();
    }

    @Override
    public void execute(Tuple tuple) {
        String url = tuple.getStringByField("url");
        pageVisitCounts.putIfAbsent(url, 0);
        pageVisitCounts.put(url, pageVisitCounts.get(url) + 1);

        System.out.printf("Received visit to page %s (total: %d)\n", url, pageVisitCounts.get(url));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("url", "userId"));
    }
}
