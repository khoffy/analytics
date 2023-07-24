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

/*
This bolt is going to contain two attributes of type HashMap allowing us to count the visit's number
for each page and each user.
 */
public class PageVisitBolt extends BaseRichBolt {
    private Integer totalVisitCount;
    private OutputCollector outputCollector;

    /*
    These three attributes are initialized during the bolt preparation
     */
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        outputCollector = collector;
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

        totalVisitCount += 1;

        outputCollector.emit(new Values(url, userId));

        System.out.printf("Received visit #%d from user %d to page %s\n", totalVisitCount, userId, url);

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("url", "userId"));
    }
}
