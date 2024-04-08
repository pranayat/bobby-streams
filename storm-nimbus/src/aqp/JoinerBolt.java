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
    
    @Override
    public void execute(TupleWindow inputWindow) {

        try {
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

            for (Tuple tuple : inputWindow.getNew()) {
                QueryGroup queryGroup = getQueryGroupByName(tuple.getStringByField("queryGroupName"));
                String tupleClusterId = tuple.getStringByField("clusterId");
                
                for (JoinQuery joinQuery : queryGroup.getJoinQueries()) {
                    String tupleStreamId = tuple.getStringByField("streamId");

                    if (!joinQuery.isWhereSatisfied(tuple)) {
                        continue;
                    }

                    Double volumeRatio = 1.0; // should be by default 1.0 for enclosed tuples
                    if (joinQuery.isTupleIntersectedByClusterForQueryRadius(tuple)) {
                        volumeRatio = joinQuery.extractVolumeRatio(tuple);
                    }
                    
                    joinQuery.addToCountSketch(tuple, volumeRatio);
                    if (tupleStreamId.equals(joinQuery.getAggregateStream())) {
                        joinQuery.addToSumSketch(tuple, volumeRatio);
                    }

                    Double tupleApproxJoinCount = joinQuery.approxJoinCount(tuple, volumeRatio);
                    double tupleApproxJoinSum = joinQuery.approxJoinSum(tuple, tupleApproxJoinCount);

                    // no point emitting if no join partners found
                    if (tupleApproxJoinCount > 0) {
                        List<Object> values = new ArrayList<Object>();
                        values.add(joinQuery.getId());
                        values.add(queryGroup.getName());
                        values.add(tupleClusterId);
                        values.add(tupleApproxJoinCount);
                        values.add(tupleApproxJoinSum);
    
                        _collector.emit(joinQuery.getId() + "-forAggregationStream", tuple, values);
                    }
                }
            }
            
            // deduct counts and sums from sketches for expired tuples
            for (Tuple expiredTuple : inputWindow.getExpired()) {
                QueryGroup queryGroup = getQueryGroupByName(expiredTuple.getStringByField("queryGroupName"));
                String tupleStreamId = expiredTuple.getStringByField("streamId");
                
                for (JoinQuery joinQuery : queryGroup.getJoinQueries()) {

                    Double volumeRatio = 1.0;
                    if (joinQuery.isTupleIntersectedByClusterForQueryRadius(expiredTuple)) {
                        volumeRatio = joinQuery.extractVolumeRatio(expiredTuple);
                    }

                    joinQuery.removeFromCountSketch(expiredTuple, volumeRatio);
                    if (tupleStreamId.equals(joinQuery.getAggregateStream())) {
                        joinQuery.removeFromSumSketch(expiredTuple, volumeRatio);
                    }
                }
            }
        } catch (Exception e) {
            System.out.println(e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        for (Query query : schemaConfig.getQueries()) {
            declarer.declareStream(query.getId() + "-forAggregationStream", new Fields("queryId", "queryGroupName", "clusterId", "tupleApproxJoinCount", "tupleApproxJoinSum"));
        }
    }
}
