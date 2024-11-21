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
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("data_spout", new RedisStreamSpout(), 1);
            // builder.setSpout(stream.getId().concat("_spout"), new Test1Spout(stream.getId()), 1);

        BoltDeclarer gridCellAssignerBolt = builder.setBolt("gridCellAssigner", new GridCellAssignerBolt(), 1);
        gridCellAssignerBolt.shuffleGrouping("data_spout");

        if (schemaConfig.getApproximate()) {
            builder.setBolt("joiner", new JoinerBolt()
                .withWindow(Count.of(10000)), 1)
                .fieldsGrouping("gridCellAssigner", new Fields("clusterId", "queryGroupName")); // GridCellAssignerBolt --> JoinerBolt
        } else {
            builder.setBolt("joiner", new JoinerBoltExact()
                .withWindow(Count.of(10000)), 1)
                .fieldsGrouping("gridCellAssigner", new Fields("clusterId", "queryGroupName")); // GridCellAssignerBolt --> JoinerBoltExact
        }

        BoltDeclarer resultBolt = builder.setBolt("result", new ResultBolt(), 1);
        
        if (schemaConfig.getApproximate()) {
            for (Query query : schemaConfig.getQueries()) {
                Stage aggregationStage = query.getAggregationStage();
                if (aggregationStage != null) {
                    BoltDeclarer aggregationBolt = builder.setBolt("aggregator", new AggregationBolt(), 1); // only 1 aggregator instance
                    aggregationBolt.allGrouping("joiner", query.getId() + "-forAggregationStream"); // JoinerBolt --> AggregationBolt
                    resultBolt.allGrouping("aggregator", query.getId() + "-aggregateResultStream"); // AggregationBolt --> ResultBolt
                }
            }
        } else {
            BoltDeclarer noResultBolt = builder.setBolt("noResult", new NoResultBolt(), 1);
            for (Query query : schemaConfig.getQueries()) {
                resultBolt.allGrouping("joiner", query.getId() + "-joinResultStream"); // JoinerBoltExact --> ResultBolt
                noResultBolt.allGrouping("joiner", query.getId() + "-noJoinResultStream");

                Stage aggregationStage = query.getAggregationStage();
                if (aggregationStage != null) {
                    BoltDeclarer aggregationBolt = builder.setBolt("aggregator", new AggregationBoltExact(), 1); // only 1 aggregator instance
                    aggregationBolt.allGrouping("joiner", query.getId() + "-forAggregationStream"); // JoinerBoltExact --> AggregationBoltExact
                    resultBolt.allGrouping("aggregator", query.getId() + "-aggregateResultStream"); // AggregationBoltExact --> ResultBolt
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
