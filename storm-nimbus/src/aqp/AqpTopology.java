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
            // builder.setSpout(stream.getId().concat("_spout"), new Test1Spout(stream.getId()), 1);
        }

        if (schemaConfig.getClustering().getType().equals("k-means")) {
            // // should have only 1 instance
            // BoltDeclarer kMeansClusterAssignerBolt = builder.setBolt("kMeansClusterAssigner",
            //         new KMeansClusterAssignerBolt().withWindow(new BaseWindowedBolt.Count(1000),
            //                 new BaseWindowedBolt.Count(1000)),
            //         1);
            // for (Stream stream : schemaConfig.getStreams()) {
            //     kMeansClusterAssignerBolt.allGrouping(stream.getId().concat("_spout"));
            // }
            // builder.setBolt("joiner", new JoinerBolt()
            // .withWindow(Count.of(1000)), 2)
            // .partialKeyGrouping("kMeansClusterAssigner", new Fields("clusterId", "queryGroupName"));

        } else {
            // can have multiple instances
            BoltDeclarer gridCellAssignerBolt = builder.setBolt("gridCellAssigner", new GridCellAssignerBolt(), 1);
            for (Stream stream : schemaConfig.getStreams()) {
                gridCellAssignerBolt.shuffleGrouping(stream.getId().concat("_spout"));
            }

            builder.setBolt("joiner", new JoinerBolt()
                .withWindow(Count.of(100)), 1)
                .fieldsGrouping("gridCellAssigner", new Fields("clusterId", "queryGroupName"));

        }

        BoltDeclarer resultBolt = builder.setBolt("result", new ResultBolt(), 1);
        BoltDeclarer noResultBolt = builder.setBolt("noResult", new NoResultBolt(), 1);
        BoltDeclarer aggregationBolt = builder.setBolt("aggregator", new AggregationBolt(), 1);

        for (Query query : schemaConfig.getQueries()) {
            resultBolt.allGrouping("joiner", query.getId() + "_joinResultStream");
            noResultBolt.allGrouping("joiner", query.getId() + "_noJoinResultStream");
            resultBolt.allGrouping("aggregator", query.getId() + "_aggregateResultStream");

            Stage aggregationStage = query.getAggregationStage();
            if (aggregationStage != null) {
                for (String groupableField : aggregationStage.getGroupableFields()) {
                    aggregationBolt.fieldsGrouping("joiner", query.getId() + "-groupBy:" + groupableField, new Fields(groupableField));
                }
            }
        }

        Config conf = new Config();
        conf.setDebug(false);
        conf.setMessageTimeoutSecs(600);

        if (args != null && args.length > 0) {
            conf.setNumWorkers(12);
            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());

        } else {

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("aqp", conf, builder.createTopology());
            Thread.sleep(6000000);

            System.out.println("End Topology");
            cluster.killTopology("aqp");
            cluster.shutdown();
        }
    }
}
