package aqp;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class JBolt extends BaseWindowedBolt {
    OutputCollector _collector;
    private SchemaConfig schemaConfig;
    List<JoinQuery> joinQueries;
    List<QueryGroup> queryGroups;

    public JBolt() {
        this.schemaConfig = SchemaConfigBuilder.build();
    }

    @Override
    public void prepare(
            Map map,
            TopologyContext topologyContext,
            OutputCollector collector) {
        _collector = collector;

        this.joinQueries = JoinQueryBuilder.build(this.schemaConfig);
        this.queryGroups = QueryGroupBuilder.build(this.joinQueries);
    }

    private QueryGroup getQueryGroupByName(String queryGroupName) {
        // TODO make this a hashmap
        return this.queryGroups.stream()
                .filter(g -> g.getName().equals(queryGroupName))
                .findFirst()
                .get();
    }

    public Boolean isTupleInQueryGroupTree(Tuple tuple, QueryGroup queryGroup) {
        Distance distance = queryGroup.getDistance();
        TupleWrapper tupleWrapper = new TupleWrapper(queryGroup.getAxisNamesSorted());
        BPlusTree bPlusTree = queryGroup.getBPlusTree();

        // check for in all clusters since we replicate tuples to adjacent clusters
        // it would have been indexed into the tree with iDistances wrt all cluster centroids it was replicated to
        for (Cluster cluster : queryGroup.getClusterMap().values()) {
            double queryTupleToCentroidDistance = distance.calculate(cluster.getCentroid(),
                    tupleWrapper.getCoordinates(tuple, queryGroup.getDistance() instanceof CosineDistance));

            List<Tuple> existingTuples = bPlusTree.search(cluster.getI() * queryGroup.getC() + queryTupleToCentroidDistance,
                    cluster.getI() * queryGroup.getC() + queryTupleToCentroidDistance);
            // multiple tuples with different ids could have the same iDistance so check the tupleID as well
            if (existingTuples.stream().anyMatch(t -> t.getStringByField("tupleId").equals(tuple.getStringByField("tupleId")))) {
                return true;
            }
        }

        return false;
    }

    private void insertIntoQueryGroupTree(Tuple tuple, QueryGroup queryGroup) {
        Distance distance = queryGroup.getDistance();
        TupleWrapper tupleWrapper = new TupleWrapper(queryGroup.getAxisNamesSorted());
        BPlusTree bPlusTree = queryGroup.getBPlusTree();

        Cluster cluster = queryGroup.getCluster(tuple.getStringByField("clusterId"));
        double queryTupleToCentroidDistance = distance.calculate(cluster.getCentroid(),
                tupleWrapper.getCoordinates(tuple, queryGroup.getDistance() instanceof CosineDistance));

        // insert tuple into B+tree by computing iDistance from tuple's cluster
        bPlusTree.insert(cluster.getI() * queryGroup.getC() + queryTupleToCentroidDistance, tuple);
        queryGroup.setBPlusTree(bPlusTree);
    }

    private List<Double> convertClusterIdToCentroid(String clusterId) {
        String[] centroidStringCoordinates = clusterId.substring(1, clusterId.length() - 1).split(",");
        List<Double> centroid = new ArrayList<>();
        for (String coordinate : centroidStringCoordinates) {
            centroid.add(Double.parseDouble(coordinate));
        }

        return centroid;
    }

    private void deleteExpiredTuplesFromTree(List<Tuple> expiredTuples) {
        for (Tuple expiredTuple : expiredTuples) {
            QueryGroup queryGroup = this.getQueryGroupByName(expiredTuple.getStringByField("queryGroupName"));
            Distance distance = queryGroup.getDistance();
            TupleWrapper tupleWrapper = new TupleWrapper(queryGroup.getAxisNamesSorted());
            Cluster cluster = queryGroup.getCluster(expiredTuple.getStringByField("clusterId"));
            BPlusTree bPlusTree = queryGroup.getBPlusTree();
            double queryTupleToCentroidDistance = distance.calculate(cluster.getCentroid(),
                    tupleWrapper.getCoordinates(expiredTuple, queryGroup.getDistance() instanceof CosineDistance));

            try {
                // deleting by iDistance key alone could result in deleting false positives, so pass tupleId as well
                bPlusTree.delete(cluster.getI() * queryGroup.getC() + queryTupleToCentroidDistance,
                        expiredTuple.getStringByField("tupleId"));
            } catch(Exception e) {
                System.out.println(e);
            }
        }
    }

    @Override
    public void execute(TupleWindow inputWindow) {
        for (Tuple tuple : inputWindow.getNew()) {
            _collector.emit(new Values("foo", "bar", "here"));
        }
        // for (Tuple tuple : inputWindow.getNew()) {
        //     QueryGroup queryGroup = this.getQueryGroupByName(tuple.getStringByField("queryGroupName"));
        //     TupleWrapper tupleWrapper = new TupleWrapper(queryGroup.getAxisNamesSorted());
        //     String clusterId = tuple.getStringByField("clusterId");
        //     Cluster cluster = queryGroup.getCluster(clusterId);

        //     if (cluster != null) {
        //         cluster.addTuple(tuple);
        //     } else {
        //         cluster = new Cluster(convertClusterIdToCentroid(clusterId), tupleWrapper);
        //         cluster.addTuple(tuple);
        //         queryGroup.setCluster(clusterId, cluster);
        //     }
        // }

        // for (Tuple tuple : inputWindow.getNew()) {
        //     QueryGroup queryGroup = this.getQueryGroupByName(tuple.getStringByField("queryGroupName"));

        //     // if we don't replicate to adjacent cells, this check is not needed, it will always return false (I verified this)
        //     // we had replicated the tuple to adjacent clusters since we didn't know which joiner bolt (partition) it would end up in
        //     // now since it might have ended up in the same joiner bolt instance, we check all clusters of this query group for this tuple
        //     // so as to insert it only once into the B+tree of this query group
        //     if (this.isTupleInQueryGroupTree(tuple, queryGroup)) {
        //         // already in tree and also hence already called joinQuery.execute on it
        //         continue;
        //     }

        //     for (JoinQuery joinQuery : queryGroup.getJoinQueries()) {
        //         List<Tuple> joinResults = joinQuery.execute(tuple, queryGroup);
        //         List<String> tupleIds = new ArrayList<>();
        //         for (Tuple joinResult : joinResults) {
        //             tupleIds.add(joinResult.getStringByField("tupleId"));
        //         }

        //         Collections.sort(tupleIds);
        //         String joinId = String.join("+", tupleIds);

        //         for (Tuple joinResult : joinResults) {
        //             _collector.emit(new Values(joinId, joinResult.getStringByField("tupleId"), joinResult.getStringByField("streamId")));
        //         }
        //     }
            
        //     // index the tuple
        //     this.insertIntoQueryGroupTree(tuple, queryGroup);
        // }

        // this.deleteExpiredTuplesFromTree(inputWindow.getExpired());
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("joinId", "tupleId", "streamId"));
    }
}
