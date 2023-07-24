package analytics;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;

/**
 * Hello world!
 *
 */
public class App {
    public static void main( String[] args ) {
        /**
         * Let's create our 1st Storm's topology (without Spout and Bolt) using a TopologyBuilder
         */
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("page-visits", new PageVisitSpout());
        //builder.setBolt("visit-counts", new PageVisitBolt()).shuffleGrouping("page-visits");

        // Let's use a model where the bolt will be executed on two workers simultaneously. Inside the console,
        // we observe that the visit number for every user is not correct.
        builder.setBolt("visit-counts", new PageVisitBolt(), 2).shuffleGrouping("page-visits");
        StormTopology topology = builder.createTopology();

        /*
        To run a tpology, we need a cluster. As we don't have any available cluster, let's use
        a local cluster to which we're going to submit the topology
         */
        LocalCluster cluster = new LocalCluster();
        Config config = new Config();
        cluster.submitTopology("analytics", config, topology);

    }
}
