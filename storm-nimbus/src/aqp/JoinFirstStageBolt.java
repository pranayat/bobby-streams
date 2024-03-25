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
import java.util.stream.Collectors;

public class JoinFirstStageBolt extends BaseWindowedBolt {
    OutputCollector _collector;
    private SchemaConfig schemaConfig;
    List<JoinQuery> joinQueries;
    List<QueryGroup> queryGroups;

    public JoinFirstStageBolt() {
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
                // deleting by iDistance key alone could result in deleting false positives, so
                // pass tupleId as well
                bPlusTree.delete(cluster.getI() * queryGroup.getC() + queryTupleToCentroidDistance,
                        expiredTuple.getStringByField("tupleId"));
            } catch (Exception e) {
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

                // if using grid indexing + cosine similarities, we'll have to normlize the
                // centroids here
                // when using spherical k-means, centroids are already normalized at the
                // clustering bolt
                Boolean normalizeCentroid = queryGroup.getDistance() instanceof CosineDistance
                        && schemaConfig.getClustering().getType().equals("grid");

                // if using cosine distance, normalize tuple coordinates when computing cluster
                // radius
                Boolean normalizeTupleCoordinates = queryGroup.getDistance() instanceof CosineDistance;

                if (cluster != null) {
                    cluster.expandRadiusWithTuple(
                            cluster.tupleWrapper.getCoordinates(tuple, normalizeTupleCoordinates));
                } else {
                    cluster = new Cluster(convertClusterIdToCentroid(clusterId, normalizeCentroid), tupleWrapper);
                    cluster.expandRadiusWithTuple(
                            cluster.tupleWrapper.getCoordinates(tuple, normalizeTupleCoordinates));
                    queryGroup.setCluster(clusterId, cluster);
                }
            }

            for (Tuple tuple : inputWindow.getNew()) {
                QueryGroup queryGroup = getQueryGroupByName(tuple.getStringByField("queryGroupName"));
                Boolean isReplica = tuple.getBooleanByField("isReplica");

                for (JoinQuery joinQuery : queryGroup.getJoinQueries()) {
                    List<List<Tuple>> validJoinCombinations = new ArrayList<>();

                    // join non-replicas with all other non-replicas in the cell
                    // so no need to use index or even calculate distances, just make sure that join partners are not in the same stream and and are not replicas
                    if (!isReplica) {
                        validJoinCombinations = joinQuery.execute(tuple, queryGroup, false, inputWindow.get(), false);
                    }

                    // for replicas we don't know if they are within join range to other replicas so
                    // need to find actual join combinations with other replicas/non-replicas
                    else {
                        List<List<Tuple>> joinCombinations = new ArrayList<>();
                        // - find join partners in index using join radius r of current query - there
                        // should be atleast one non-replica tuple in the join result combination
                        joinCombinations = joinQuery.execute(tuple, queryGroup, true, null, null);

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
                    }

                    if (validJoinCombinations.isEmpty()) {
                        // but should ideally find join partners unless index is still being populated or maybe the data distribution is such that all tuples are from same stream
                        // or ar all replicas
                        _collector.emit(joinQuery.getId() + "_noResultStream", tuple, new Values(joinQuery.getId()));
                    }

                    int i = 0;
                    for (List<Tuple> validJoinCombination : validJoinCombinations) {
                        i++;

                        List<String> joinCombinationTupleIds = new ArrayList<String>();
                        for (Tuple t : validJoinCombination) {
                            joinCombinationTupleIds.add(t.getStringByField("tupleId"));
                        }
                        Collections.sort(joinCombinationTupleIds);
                        String joinCombinationId = String.join("-", joinCombinationTupleIds);

                        for (Tuple joinTuple : validJoinCombination) {

                            Stream tupleStream = this.schemaConfig.getStreamById(joinTuple.getStringByField("streamId"));
                            List<Object> values = new ArrayList<Object>();
                            for (Field field : tupleStream.getFields()) {
                                if (field.getType().equals("double")) {
                                    values.add(joinTuple.getDoubleByField(field.getName()));
                                } else {
                                    values.add(joinTuple.getStringByField(field.getName()));
                                }
                            }

                            values.add(joinTuple.getStringByField("streamId"));
                            values.add(joinCombinationId);

                            // emit a tuple in a join combintaion T1_S1 - T2_S2 - T3_S3
                            // each query has its own result stream
                            // each join results is anchored by the input tuple that triggered the join 
                            _collector.emit(joinQuery.getId() + "_resultStream", tuple, new Values(values.toArray()));
                        }
                    }
                }

                insertIntoQueryGroupTree(tuple, queryGroup);
            }

            deleteExpiredTuplesFromTree(inputWindow.getExpired());
        } catch (Exception e) {
            System.out.println(e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        for (Query query : schemaConfig.getQueries()) {
            declarer.declareStream(query.getId() + "_noResultStream", new Fields("queryId"));
            
            Stream stream = this.schemaConfig.getStreams().get(0);
            List<String> fields = new ArrayList<String>(stream.getFieldNames());
            fields.add("streamId");
            fields.add("joinId");
            declarer.declareStream(query.getId() + "_resultStream", new Fields(fields));
        }
    }
}
