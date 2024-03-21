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

public class JoinerBoltNew extends BaseWindowedBolt {
    OutputCollector _collector;
    private SchemaConfig schemaConfig;
    List<JoinQuery> joinQueries;
    List<QueryGroup> queryGroups;

    public JoinerBoltNew() {
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

    private void insertIntoQueryGroupTree(Tuple tuple, QueryGroup queryGroup) {
        Distance distance = queryGroup.getDistance();
        TupleWrapper tupleWrapper = new TupleWrapper(queryGroup.getAxisNamesSorted());
        BPlusTreeNew bPlusTree = queryGroup.getBPlusTree();

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
            BPlusTreeNew bPlusTree = queryGroup.getBPlusTree();
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
                String tupleStreamId = tuple.getStringByField("streamId");
                Boolean isReplica = tuple.getBooleanByField("isReplica");
                
                for (JoinQuery joinQuery : queryGroup.getJoinQueries()) {
                    String querySumStreamId = joinQuery.getSumStream();
                    String querySumField = joinQuery.getSumField();
                    int queryJoinCountForCluster = 0;
                    double queryJoinSumForCluster = 0;

                    if (!isReplica) {
                        // count_min(C1_S1) ++
                        // count_min(C1_S1) ++
                        joinQuery.getPanakosCountSketch().add(tupleClusterId + "_" + tupleStreamId);

                        // count_min_sum(C1_S1) += S1.velocity for query SUM(S1.velocity)
                        if (tupleStreamId.equals(querySumStreamId)) {
                            joinQuery.getPanakosSumSketch().add(tupleClusterId + "_" + tupleStreamId, tuple.getDoubleByField(querySumField));
                        }

                        // join_count_C1_query1 = count_min(C1_S1) x count_min(C1_S2) x count_min(C1_S3), query1 = JOIN(S1 x S2 x S3)
                        queryJoinCountForCluster = 1;
                        for (String streamId : joinQuery.getStreamIds()) {
                            // don't join this tuple with it's own stream
                            if (streamId.equals(tupleStreamId)) {
                                queryJoinCountForCluster *= joinQuery.getPanakosCountSketch().query(tupleClusterId + "_" + streamId);
                            }
                        }

                        // no point computing sum if no join tuples
                        if (queryJoinCountForCluster != 0 && joinQuery.getPanakosCountSketch().query(tupleClusterId + "_" + querySumStreamId) != 0) {
                            // query1 = SUM(S1.value)
                            // join_sum_C1_query1 = [ join_count_C1_query1 / count_min(C1_S1) ] x count_min_sum(C1_S1)
                            queryJoinSumForCluster = (queryJoinCountForCluster / joinQuery.getPanakosCountSketch().query(tupleClusterId + "_" + querySumStreamId))
                                    * joinQuery.getPanakosSumSketch().query(tupleClusterId + "_" + querySumStreamId);
                        }
                    }
                    
                    // isReplica = true
                    else {
                        // - find join partners in index using join radius r of current query - there should be atleast one non-replica tuple in the join result combination
                        List<List<Tuple>> joinCombinations = joinQuery.execute(tuple, queryGroup);
                        List<List<Tuple>> validJoinCombinations = new ArrayList<>();

                        for (List<Tuple> joinCombination : joinCombinations) {

                            Boolean atleastOneNonReplica = false;
                            for (Tuple joinPartner : joinCombination) {

                                if (!joinPartner.getBooleanByField("isReplica")) {
                                    atleastOneNonReplica = true;
                                    break;
                                }
                            }

                            if (atleastOneNonReplica) {
                                validJoinCombinations.add(joinCombination);
                            }
                        }

                        queryJoinCountForCluster = 0;
                        queryJoinSumForCluster = 0;
                        for (List<Tuple> validJoinCombination : validJoinCombinations) {
                            queryJoinCountForCluster += 1; // increment for this join combination of tuples

                            for (Tuple joinPartner : validJoinCombination) {

                                // will be true once per join combination
                                if (joinPartner.getStringByField("streamId").equals(querySumStreamId)) {
                                    queryJoinSumForCluster += joinPartner.getDoubleByField(querySumField);
                                }
                            }
                        }
                    }

                    // no point emitting if no join partners found
                    if (queryJoinCountForCluster > 0) {
                        List<Object> values = new ArrayList<Object>();
                        values.add(joinQuery.getId());
                        values.add(tupleClusterId);
                        values.add(queryJoinCountForCluster);
                        values.add(queryJoinSumForCluster);
    
                        _collector.emit("aggregateStream", tuple, values);
                    }
                }
            
                insertIntoQueryGroupTree(tuple, queryGroup);
            }
        
            deleteExpiredTuplesFromTree(inputWindow.getExpired());
            
            // deduct counts and sums from sketches for expired tuples
            for (Tuple expiredTuple : inputWindow.getExpired()) {
                QueryGroup queryGroup = getQueryGroupByName(expiredTuple.getStringByField("queryGroupName"));
                Boolean isReplica = expiredTuple.getBooleanByField("isReplica");
                String tupleClusterId = expiredTuple.getStringByField("clusterId");
                String tupleStreamId = expiredTuple.getStringByField("streamId");
                
                for (JoinQuery joinQuery : queryGroup.getJoinQueries()) {
                    String querySumStreamId = joinQuery.getSumStream();
                    String querySumField = joinQuery.getSumField();

                    if (!isReplica) {
                        joinQuery.getPanakosCountSketch().add(tupleClusterId + "_" + tupleStreamId, -1);
                    }

                    if (!isReplica && tupleStreamId.equals(querySumStreamId)) {
                        joinQuery.getPanakosSumSketch().add(tupleClusterId + "_" + tupleStreamId, -expiredTuple.getDoubleByField(querySumField));
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
            declarer.declareStream(query.getId() + "_resultStream", new Fields("queryId", "tupleId", "streamId"));
            declarer.declareStream(query.getId() + "_noResultStream", new Fields("queryId", "tupleId", "streamId"));
        }

        declarer.declareStream("aggregateStream", new Fields("queryId", "clusterId", "queryJoinCountForCluster", "queryJoinSumForCluster"));
    }
}
