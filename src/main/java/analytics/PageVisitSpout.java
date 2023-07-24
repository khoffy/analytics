package analytics;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

/*
Let's create a Spout that generates log lines indicating users' access.
This spout is going to produce messages containing two fields:
- an url
- an user identifier
*/
public class PageVisitSpout extends BaseRichSpout {

    private SpoutOutputCollector outputCollector;

    /*
    while opening this spout, it stores a SpoutOutputCollector's instance
     */
    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector collector) {
        outputCollector = collector;
    }

    /*
    For every call to the method nextTuple(), the spout is going to emit an new tuple that is constituted of
    an URL and an userID picked randomly in two lists.
    Tuples produced by this spout are going to send by a bolt that is going to count the total number of visit
    for each page and each user. Let's develop this bolt "PageVisitBolt"
     */
    @Override
    public void nextTuple() {
        String[] urls = {"http://example.com/index.html", "http://example.com/404.html", "http://example.com/"};
        Integer[] userIds = {1, 2, 3, 5};

        String url = urls[ThreadLocalRandom.current().nextInt(urls.length)];
        Integer userId = userIds[ThreadLocalRandom.current().nextInt(userIds.length)];

        outputCollector.emit(new Values(url, userId));
        Utils.sleep(2000); // simulate a 2 seconds break between two access.
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("url", "userId"));
    }
}
