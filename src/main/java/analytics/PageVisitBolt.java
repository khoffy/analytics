package analytics;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

/*
This bolt is going to contain two attributes of type HashMap allowing us to count the visit's number
for each page and each user.
 */
public class PageVisitBolt extends BaseRichBolt {
    private HashMap<String, Integer> pageVisitCounts;
    private HashMap<String, Integer> userVisitCounts;
    private Integer totalVisitCount;

    /*
    These three attributes are initialized during the bolt preparation
     */
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        pageVisitCounts = new HashMap<>();
        userVisitCounts = new HashMap<>();
        totalVisitCount = 0;
    }

    /*
    Each visit will trigger execute() method call and displays a message in the console.
    Next, let's connect the spout and the bolt in main method
     */
    @Override
    public void execute(Tuple tuple) {
        String url = tuple.getStringByField("url");
        Integer userId = tuple.getIntegerByField("userId");

        pageVisitCounts.putIfAbsent(url, 0);
        userVisitCounts.putIfAbsent(userId.toString(), 0);
        totalVisitCount += 1;

        System.out.printf("Received visit #%d from user %d (total: %d) to page %s (total: %d)\n",
                totalVisitCount, userId, userVisitCounts.get(userId), url, pageVisitCounts.get(url));


    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
