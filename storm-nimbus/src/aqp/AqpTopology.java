package aqp;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.BoltDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.topology.base.BaseWindowedBolt.Count;
import org.apache.storm.tuple.Fields;

public class AqpTopology {
    public static void main(String[] args) throws Exception {

        SchemaConfig schemaConfig = SchemaConfigBuilder.build();

        // data/redis -> gridCellAssigner -> (partitionAssigner) -> joiner
        TopologyBuilder builder = new TopologyBuilder();

        for (Stream stream : schemaConfig.getStreams()) {
            builder.setSpout(stream.getId().concat("_spout"), new RedisStreamSpout(stream.getId()), 1);
        }

        if (schemaConfig.getClustering().getType().equals("k-means")) {
            // should have only 1 instance
            BoltDeclarer kMeansClusterAssignerBolt = builder.setBolt("kMeansClusterAssigner",
                    new KMeansClusterAssignerBolt().withWindow(new BaseWindowedBolt.Count(100),
                            new BaseWindowedBolt.Count(100)),
                    1);
            for (Stream stream : schemaConfig.getStreams()) {
                kMeansClusterAssignerBolt.allGrouping(stream.getId().concat("_spout"));
            }
        } else {
            // can have multiple instances as entire data is not needed for clustering
            BoltDeclarer gridCellAssignerBolt = builder.setBolt("gridCellAssigner", new GridCellAssignerBolt(), 1);
            for (Stream stream : schemaConfig.getStreams()) {
                gridCellAssignerBolt.shuffleGrouping(stream.getId().concat("_spout"));
            }
        }

        // can have multiple instances, but not more than the number of clusters
        builder.setBolt("jBolt", new JBolt()
            .withWindow(Count.of(30)), 1)
            .partialKeyGrouping("gridCellAssigner", new Fields("clusterId", "queryGroupName"));

        builder.setBolt("printer", new PrinterBolt(), 1).shuffleGrouping("jBolt");

        Config conf = new Config();
        conf.setDebug(true);
        conf.setMessageTimeoutSecs(600);

        if (args != null && args.length > 0) {
            conf.setNumWorkers(4);
            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());

        } else {

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("aqp", conf, builder.createTopology());
            Thread.sleep(6000000);

            System.out.println("End Topology");
            // cluster.killTopology("aqp");
            // cluster.shutdown();
        }
    }
}
