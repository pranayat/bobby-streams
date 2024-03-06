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
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class JoinerBolt extends BaseWindowedBolt {
    OutputCollector _collector;
    private SchemaConfig schemaConfig;
    List<JoinQuery> joinQueries;
    List<QueryGroup> queryGroups;

    public JoinerBolt() {
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

    private List<Double> convertClusterIdToCentroid(String clusterId, Boolean normalize) {
        String[] centroidStringCoordinates = clusterId.substring(1, clusterId.length() - 1).split(",");
        List<Double> centroid = new ArrayList<>();
        for (String coordinate : centroidStringCoordinates) {
            centroid.add(Double.parseDouble(coordinate));
        }

        if (normalize) {
            double magnitude = Math.sqrt(centroid.stream().mapToDouble(x -> x * x).sum());
            if (magnitude == 0) {
                return centroid;
            }
            return centroid.stream().map(x -> x / magnitude).collect(Collectors.toList());
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
            QueryGroup queryGroup = this.getQueryGroupByName(tuple.getStringByField("queryGroupName"));
            TupleWrapper tupleWrapper = new TupleWrapper(queryGroup.getAxisNamesSorted());
            String clusterId = tuple.getStringByField("clusterId");
            Cluster cluster = queryGroup.getCluster(clusterId);
            
            // if using grid indexing + cosine similarities, we'll have to normlize the centroids here
            // when using spherical k-means, centroids are already normalized at the clustering bolt
            Boolean normalizeCentroid = queryGroup.getDistance() instanceof CosineDistance && schemaConfig.getClustering().getType().equals("grid");
            
            // if using cosine distance, normalize tuple coordinates when computing cluster radius
            Boolean normalizeTupleCoordinates = queryGroup.getDistance() instanceof CosineDistance;

            if (cluster != null) {
                cluster.expandRadiusWithTuple(cluster.tupleWrapper.getCoordinates(tuple, normalizeTupleCoordinates));
            } else {
                cluster = new Cluster(convertClusterIdToCentroid(clusterId, normalizeCentroid), tupleWrapper);
                cluster.expandRadiusWithTuple(cluster.tupleWrapper.getCoordinates(tuple, normalizeTupleCoordinates));
                queryGroup.setCluster(clusterId, cluster);
            }
        }

        // getNew() will return the one new tuple that slides in
        for (Tuple tuple : inputWindow.getNew()) {
            QueryGroup queryGroup = getQueryGroupByName(tuple.getStringByField("queryGroupName"));

            // needed only if replicating to  adjacent cells
            // if (isTupleInQueryGroupTree(tuple, queryGroup)) {
            //     continue;
            // }

            for (JoinQuery joinQuery : queryGroup.getJoinQueries()) {
                List<Tuple> joinResults = joinQuery.execute(tuple, queryGroup);
                // List<String> tupleIds = new ArrayList<>();
                // for (Tuple joinResult : joinResults) {
                //     tupleIds.add(joinResult.getStringByField("tupleId"));
                // }

                // Collections.sort(tupleIds);
                // String joinId = String.join("+", tupleIds);

                for (Tuple joinResult : joinResults) {
                    _collector.emit("resultStream", tuple, new Values(joinResult.getStringByField("tupleId"), joinResult.getStringByField("streamId")));
                }

                if (joinResults.size() == 0) {
                    _collector.emit("noResultStream", tuple, new Values(tuple.getStringByField("tupleId"), tuple.getStringByField("streamId")));
                }
            }
            
            // index the tuple
            insertIntoQueryGroupTree(tuple, queryGroup);
        }

        // TODO shrink cluster radius, and what if we don't do it ?
        deleteExpiredTuplesFromTree(inputWindow.getExpired());
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("resultStream", new Fields("tupleId", "streamId"));
        declarer.declareStream("noResultStream", new Fields("tupleId", "streamId"));
    }
}
