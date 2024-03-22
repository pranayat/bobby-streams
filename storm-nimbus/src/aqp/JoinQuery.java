package aqp;

import org.apache.storm.tuple.Tuple;

import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.stream.Collectors;

public class JoinQuery {
    String id;
    double radius;
    List<String> streamIds;
    List<String> fields;
    List<List<Tuple>> results;
    Distance distance;
    IDistance iDistance;
    Panakos panakosSumSketch;
    String sumStream;
    String sumField;
    Panakos panakosCountSketch;
    Map<String, Integer> clusterJoinCountMap;
    Map<String, Double> clusterJoinSumMap;

    public JoinQuery(String id, double radius, List<String> streamIds, List<String> fields, Distance distance, IDistance iDistance) {
        Collections.sort(fields);
        this.id = id;
        this.radius = radius;
        this.streamIds = streamIds;
        this.fields = fields;
        this.results = new ArrayList<>();
        this.distance = distance;
        this.iDistance = iDistance;
        this.panakosCountSketch = new Panakos();
        this.panakosSumSketch = new Panakos();
        this.clusterJoinCountMap = new HashMap<>();
        this.clusterJoinSumMap = new HashMap<>();
    }

    public void removeFromCountSketch(Tuple tuple) throws NoSuchAlgorithmException {
        String tupleClusterId = tuple.getStringByField("clusterId");
        String tupleStreamId = tuple.getStringByField("streamId");

        this.panakosCountSketch.remove(tupleClusterId + "_" + tupleStreamId, 1);

    }

    public void removeFromSumSketch(Tuple tuple) throws NoSuchAlgorithmException {
        String tupleClusterId = tuple.getStringByField("clusterId");
        String tupleStreamId = tuple.getStringByField("streamId");

        this.panakosSumSketch.remove(tupleClusterId + "_" + tupleStreamId, tuple.getDoubleByField(this.sumField));
    }

    public void addToCountSketch(Tuple tuple) throws NoSuchAlgorithmException {
        String tupleClusterId = tuple.getStringByField("clusterId");
        String tupleStreamId = tuple.getStringByField("streamId");

        // count_min(C1_S1) ++
        // count_min(C1_S1) ++
        this.panakosCountSketch.add(tupleClusterId + "_" + tupleStreamId, 1);
    }

    public void addToSumSketch(Tuple tuple) throws NoSuchAlgorithmException {
        String tupleClusterId = tuple.getStringByField("clusterId");
        String tupleStreamId = tuple.getStringByField("streamId");

        // count_min_sum(C1_S1) += S1.velocity for query SUM(S1.velocity)
        this.panakosSumSketch.add(tupleClusterId + "_" + tupleStreamId, tuple.getDoubleByField(this.sumField));
    }

    public Integer approxJoinCount(Tuple tuple) throws NoSuchAlgorithmException {
        String tupleClusterId = tuple.getStringByField("clusterId");
        String tupleStreamId = tuple.getStringByField("streamId");

        // join_count_C1_query1 = count_min(C1_S1) x count_min(C1_S2) x count_min(C1_S3), query1 = JOIN(S1 x S2 x S3)
        Integer tupleApproxJoinCount = 1;
        for (String streamId : this.streamIds) {
            // join this tuple with other streams in this cell
            if (streamId.equals(tupleStreamId)) {
                tupleApproxJoinCount *= this.panakosCountSketch.query(tupleClusterId + "_" + streamId);
            }
        }

        return tupleApproxJoinCount;
    }

    public Double approxJoinSum(Tuple tuple, Integer tupleApproxJoinCount) throws NoSuchAlgorithmException {
        String tupleClusterId = tuple.getStringByField("clusterId");
        double tupleApproxJoinSum = 0;

        // no point computing sum if no join tuples
        if (tupleApproxJoinCount != 0 && this.panakosCountSketch.query(tupleClusterId + "_" + this.sumStream) != 0) {
            // query1 = SUM(S1.value)
            // join_sum_C1_query1 = [ join_count_C1_query1 / count_min(C1_S1) ] x count_min_sum(C1_S1)
            tupleApproxJoinSum = (tupleApproxJoinCount / this.panakosCountSketch.query(tupleClusterId + "_" + this.sumStream))
                    * this.panakosSumSketch.query(tupleClusterId + "_" + this.sumStream);
        }
        
        return tupleApproxJoinSum;
    }
    
    public Integer aggregateJoinCounts() {
        return this.clusterJoinCountMap.values().stream().mapToInt(Integer::intValue).sum();
    }

    public Double aggregateJoinSums() {
        return this.clusterJoinSumMap.values().stream().mapToDouble(Double::doubleValue).sum();
    }

    public Map<String, Integer> getClusterJoinCountMap() {
        return this.clusterJoinCountMap;
    }

    public Map<String, Double> getClusterJoinSumMap() {
        return this.clusterJoinSumMap;
    }    

    public Panakos getPanakosCountSketch() {
        return this.panakosCountSketch;
    }

    public Panakos getPanakosSumSketch() {
        return this.panakosSumSketch;
    }
    
    public void setSumStream(String sumStream) {
        this.sumStream = sumStream;
    }

    public void setSumField(String sumField) {
        this.sumField = sumField;
    }

    public String getSumStream() {
        return this.sumStream;
    }

    public String getSumField() {
        return this.sumField;
    }

    public String getId() {
        return this.id;
    }

    public double getRadius() {
        return this.radius;
    }

    public List<String> getStreamIds() {
        return this.streamIds;
    }

    public List<String> getFieldsSorted() {
        Collections.sort(this.fields);
        return this.fields;
    }

    public Distance getDistance() {
        return this.distance;
    }

    public IDistance getIDistance() {
        return this.iDistance;
    }

    private List<Tuple> findJoinPartnersInStreamNoIndex(Tuple tuple, QueryGroup queryGroup, String streamToJoin, List<Tuple> window, Boolean calculateDistance) {
        TupleWrapper tupleWrapper = new TupleWrapper(queryGroup.getAxisNamesSorted());
        List<Tuple> joinCandidatesFromOtherStreams = new ArrayList<>();
        for (Tuple joinCandidate : window) {
            if (streamToJoin.equals(joinCandidate.getStringByField("streamId"))) {
                
                // use this when doing non-replica to non-replica joins, since all non-replicas are joinable within a cell
                // so no need to compute distances for these joins, just make sure that join partners are not in the same stream and and are not replicas
                if (!calculateDistance) {
                    if (!joinCandidate.getBooleanByField("isReplica")) {
                        joinCandidatesFromOtherStreams.add(joinCandidate);
                    }
                }
                
                else if (this.distance.calculate(tupleWrapper.getCoordinates(tuple, this.distance instanceof CosineDistance),
                    tupleWrapper.getCoordinates(joinCandidate, this.distance instanceof CosineDistance)) <= this.getRadius()) {
                        joinCandidatesFromOtherStreams.add(joinCandidate);
                }
            }
        }

        return joinCandidatesFromOtherStreams;
    }

    private List<Tuple> findJoinPartnersInStream(Tuple tuple, QueryGroup queryGroup, String streamToJoin) {
        BPlusTreeNew<Double, Tuple> bPlusTree = queryGroup.getBPlusTree();
        TupleWrapper tupleWrapper = new TupleWrapper(queryGroup.getAxisNamesSorted());
        List<Tuple> joinCandidates = new ArrayList<>();

        // only need to search for join partners within a cluster as everything joinable is already inside it
        Cluster cluster = queryGroup.getCluster(tuple.getStringByField("clusterId"));
        IDistance iDistance = queryGroup.getIDistance();
        List<Double> searchBounds = iDistance.getSearchBounds(cluster.getRadius(), cluster.getCentroid(),
                cluster.getI(), queryGroup.getC(), this.getRadius(), tupleWrapper.getCoordinates(tuple, this.distance instanceof CosineDistance));

        if (!Double.isNaN(searchBounds.get(0)) && !Double.isNaN(searchBounds.get(1))) {
            joinCandidates.addAll(bPlusTree.searchRange(searchBounds.get(0), BPlusTreeNew.RangePolicy.INCLUSIVE, searchBounds.get(1), BPlusTreeNew.RangePolicy.INCLUSIVE));
        }

        List<Tuple> joinCandidatesFromOtherStreams = new ArrayList<>();
        for (Tuple joinCandidate : joinCandidates) {
            if (streamToJoin.equals(joinCandidate.getStringByField("streamId"))) {
                // iDistance has false positives as the index is simply distance from cluster center
                boolean isFalsePositive = this.distance.calculate(tupleWrapper.getCoordinates(tuple, this.distance instanceof CosineDistance),
                        tupleWrapper.getCoordinates(joinCandidate, this.distance instanceof CosineDistance)) > this.getRadius();

                if (!isFalsePositive) {
                    joinCandidatesFromOtherStreams.add(joinCandidate);
                } else {
                    // TODO collect some stats here for iDistance false positives
                }
            }
        }

        return joinCandidatesFromOtherStreams;
    }

    private List<Tuple> findCommonElements(List<List<Tuple>> subarrays) {
        List<Tuple> commonElements = new ArrayList<>();
  
        // Start with the first subarray
        if (!subarrays.isEmpty()) {
            commonElements.addAll(subarrays.get(0));
  
            // Iterate through the remaining subarrays
            for (int i = 1; i < subarrays.size(); i++) {
                List<Tuple> currentSubarray = subarrays.get(i);
                commonElements.retainAll(currentSubarray); // Retain only elements present in both lists
            }
        }
  
        return commonElements;
    }

    public List<List<Tuple>> execute(Tuple tuple, QueryGroup queryGroup, Boolean useIndex, List<Tuple> window, Boolean calculateDistance) {
        List<String> streamsToJoin = this.streamIds.stream()
                .filter(s -> !s.equals(tuple.getStringByField("streamId"))).collect(Collectors.toList()); // [b, c]

        // [ [a1] ]
        // for incoming query tuple a1 in stream a, find join partners in stream b - [b1, b2]
        // create one join combination per join partner in b - [ [a1, b1], [a1, b2] ]
        // for each element in each join combination, find join partners in c
          // for [a1, b1] => a1 - c1, c2, c3 and b1 - c1, c2
            // => common is c1, c2 so create one combination with each of the common partners => [a1, b1, c1] [a1, b1, c2]
          // for [a1, b2] => a1 - c1, c2, c3 and b2 - c3, c4
            // => common is only c3 so create one combination with each of the common partners => [a1, b2, c3]
        // => [ [a1, b1, c1], [a1, b1, c2], [a1, b2, c3] ]

        Queue<List<Tuple>> joinCombinations = new ArrayDeque<>();
        List<Tuple> firstCombination = new ArrayList<Tuple>();
        firstCombination.add(tuple);
        joinCombinations.offer(firstCombination); // [ [a1] ]

        List<List<Tuple>> resultCombinations = new ArrayList<>();

        int j = 0;
        while (!joinCombinations.isEmpty()) { // for [a1, b1] in [ [a1, b1], [a1, b2] ]

            if (j == streamsToJoin.size()) {
                break;
            }

            int levelSize = joinCombinations.size();
            String streamToJoin = streamsToJoin.get(j);

            for (int i = 0; i < levelSize; i++) {

                List<Tuple> joinCombination = joinCombinations.poll(); // [a1, b1]
                List<List<Tuple>> combinationJoinPartners = new ArrayList<>();

                for (Tuple element: joinCombination) { // for a1 in [a1, b1]
                    List<Tuple> elementJoinPartners = useIndex ? this.findJoinPartnersInStream(element, queryGroup, streamToJoin) : this.findJoinPartnersInStreamNoIndex(element, queryGroup, streamToJoin, window, calculateDistance); // element a1 - [c1, c2, c3], elment b1 - [c1, c2]
                    combinationJoinPartners.add(elementJoinPartners); // [a1, b1] - [ [c1, c2, c3], [c1, c2] ]
                }

                List<Tuple> combinationCommonJoinPartners = findCommonElements(combinationJoinPartners); // for [a1, b1] common are [c1, c2]

                // create a new join extended combination for each of the common join partners c1 and c2 with the combination [a1, b1] => [a1, b1, c1], [a1, b1, c2]
                for (Tuple combinationCommonJoinPartner : combinationCommonJoinPartners) { // for c1 in [c1, c2]
                    List<Tuple> extendedJoinCombination = new ArrayList<>();
                    extendedJoinCombination.addAll(joinCombination);
                    extendedJoinCombination.add(combinationCommonJoinPartner); // [a1, b1] got extended to [a1, b1, c1]
                    joinCombinations.offer(extendedJoinCombination); // [ [a1] [a1, b1], [a1, b2], [a1, b1, c1] ]
                }

                // we just joined the last stream so add the join combinations
                if (j == streamsToJoin.size() - 1) {
                    resultCombinations.addAll(joinCombinations);
                }
            }

            j++; // done with this level, join next stream at next level
        }

        return resultCombinations;
    }
}
