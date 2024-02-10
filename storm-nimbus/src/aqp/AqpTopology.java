package aqp;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.BoltDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;

public class AqpTopology {
    public static void main(String[] args) throws Exception {

        SchemaConfig schemaConfig = SchemaConfigBuilder.build();

        // data -> gridCellAssigner -> partitionAssigner -> joiner
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("redis", new RedisStreamSpout(), 1);
//        builder.setSpout("data", new DataSpout(), 1);

        if (schemaConfig.getClustering().getType().equals("k-means")) {
            // should have only 1 instance
            BoltDeclarer kMeansClusterAssignerBolt = builder.setBolt("kMeansClusterAssigner", new KMeansClusterAssignerBolt().withWindow(new BaseWindowedBolt.Count(100), new BaseWindowedBolt.Count(100)), 1);
            for (Stream stream : schemaConfig.getStreams()) {
                kMeansClusterAssignerBolt.allGrouping("redis", stream.getId());
            }
        } else {
            // can have multiple instances as entire data is not needed for clustering
            BoltDeclarer gridCellAssignerBolt = builder.setBolt("gridCellAssigner", new GridCellAssignerBolt(), 1);
            for (Stream stream : schemaConfig.getStreams()) {
                gridCellAssignerBolt.shuffleGrouping("redis", stream.getId());
            }
        }

        // should have only 1 instance
        BoltDeclarer partitionAssignerBolt = builder.setBolt("partitionAssigner", new PartitionAssignerBolt(), 1);
        for (Stream stream : schemaConfig.getStreams()) {
            // use either this or kMeansClusterAssignerBolt, not both
            partitionAssignerBolt.allGrouping("gridCellAssigner", stream.getId());
//            partitionAssignerBolt.allGrouping("kMeansClusterAssigner", stream.getId());
        }

        // can have multiple instances, but not more than the number of clusters
        BoltDeclarer joinerBolt = builder.setBolt("joiner", new JoinerBolt().withWindow(new BaseWindowedBolt.Count(10), new BaseWindowedBolt.Count(1)), 1);
        for (Stream stream : schemaConfig.getStreams()) {
            joinerBolt.fieldsGrouping("partitionAssigner", stream.getId(), new Fields("clusterId", "queryGroupName"));
        }

        Config conf = new Config();
        conf.setDebug(false);

        if (args != null && args.length > 0) {

            // run it in a live cluster

            // set the number of workers for running all spout and bolt tasks
            // If we have two supervisors with 4 allocated workers each, and this topology is
            // submitted to the master (Nimbus) node, then these 8 workers will be distributed
            // to the two supervisors evenly: four each.
            conf.setNumWorkers(8);

            // create the topology and submit with config
            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());

        } else {

            // run it in a simulated local cluster

            // create the local cluster instance
            LocalCluster cluster = new LocalCluster();

            // submit the topology to the local cluster
            cluster.submitTopology("aqp", conf, builder.createTopology());

            // let the topology run for 100 minutes. note topologies never terminate!
            Thread.sleep(6000000);

//            cluster.killTopology("aqp");

            // we are done, so shutdown the local cluster
//            cluster.shutdown();
        }
    }
}
