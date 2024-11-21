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
    List<String> aggregatableFields;
    String aggregateStream;
    Panakos panakosCountSketch;
    List<Clause> whereClauses;
    Double exactCount = 0.0;
    Double exactSum = 0.0;

    Map<String, Double> clusterJoinCountMap;
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

    public void setWhereClauses(List<Clause> clauses) {
        this.whereClauses = clauses;
    }

    public List<Clause> getWhereClauses() {
        return this.whereClauses;
    }

    public Boolean isWhereSatisfied(Tuple tuple) {
        Boolean satisfied = true;

        for (Clause whereClause : whereClauses) {
            if (!whereClause.getStream().equals(tuple.getStringByField("streamId"))) {
                continue;
            }
            
            // for == do a string comparison
            if (whereClause.getOperator().equals("eq")) {
                String tupleValue = String.valueOf(tuple.getValueByField(whereClause.getField()));
                String givenValue = String.valueOf(whereClause.getValue());

                satisfied = satisfied && tupleValue.equals(givenValue);
            } else {
                // for >, >=, <, <= do double comparisons
                Double tupleValue = tuple.getDoubleByField(whereClause.getField());
                Double givenValue = Double.parseDouble(whereClause.getValue());
    
                if (whereClause.getOperator().equals("gt")) {
                    satisfied = satisfied && tupleValue > givenValue;
                }
    
                else if (whereClause.getOperator().equals("gte")) {
                    satisfied = satisfied && tupleValue >= givenValue;
                }
    
                else if (whereClause.getOperator().equals("lt")) {
                    satisfied = satisfied && tupleValue < givenValue;
                }
    
                else if (whereClause.getOperator().equals("lte")) {
                    satisfied = satisfied && tupleValue <= givenValue;
                }
            }
        }

        return satisfied;
    }

    //////// Exact - exact iDistance joins ///////////////////////////////// These take in the join result tuples after doing actual iDistance join
    public void addToExactCount() {
        this.exactCount++;
    }

    public void addToExactSum(Tuple tuple) {
        String aggregateField = this.aggregatableFields.get(0); // TODO do this with one sketch per aggregate field

        this.exactSum += tuple.getDoubleByField(aggregateField);
    }

    public void removeFromExactCount() {
        this.exactCount--;
    }

    public void removeFromExactSum(Tuple tuple) {
        String aggregateField = this.aggregatableFields.get(0); // TODO do this with one sketch per aggregate field

        this.exactSum -= tuple.getDoubleByField(aggregateField);
    }

    public Double getExactCount() {
        return this.exactCount;
    }

    public Double getExactSum() {
        return this.exactSum;
    }
    /////////////////////////////////////////

    public Double extractVolumeRatio(Tuple tuple) {
        int i = tuple.getStringByField("intersectedBy").indexOf(this.id);

        return Double.parseDouble(tuple.getStringByField("intersectedBy").substring(i).split("=")[1]);
    }

    public void removeFromCountSketch(Tuple tuple, Double volumeRatio) throws NoSuchAlgorithmException {
        String tupleClusterId = tuple.getStringByField("clusterId");
        String tupleStreamId = tuple.getStringByField("streamId");

        this.panakosCountSketch.remove(tupleClusterId + "_" + tupleStreamId, volumeRatio);

        if (tuple.getBooleanByField("isReplica")) {
            this.panakosSumSketch.remove(tupleClusterId + "_" + tupleStreamId + "_replica", volumeRatio);
        }
    }

    public void removeFromSumSketch(Tuple tuple, Double volumeRatio) throws NoSuchAlgorithmException {
        String tupleClusterId = tuple.getStringByField("clusterId");
        String tupleStreamId = tuple.getStringByField("streamId");
        String aggregateField = this.aggregatableFields.get(0); // TODO do this with one sketch per aggregate field

        this.panakosSumSketch.remove(tupleClusterId + "_" + tupleStreamId, volumeRatio * tuple.getDoubleByField(aggregateField));

        if (tuple.getBooleanByField("isReplica")) {
            this.panakosSumSketch.remove(tupleClusterId + "_" + tupleStreamId + "_replica", volumeRatio * tuple.getDoubleByField(aggregateField));
        }
    }

    public void addToCountSketch(Tuple tuple, Double volumeRatio) throws NoSuchAlgorithmException {
        String tupleClusterId = tuple.getStringByField("clusterId");
        String tupleStreamId = tuple.getStringByField("streamId");
        // count_min(C1_S1) ++
        // count_min(C1_S1) ++
        this.panakosCountSketch.add(tupleClusterId + "_" + tupleStreamId, volumeRatio); // TOOD add where clause

        if (tuple.getBooleanByField("isReplica")) {
            this.panakosSumSketch.add(tupleClusterId + "_" + tupleStreamId + "_replica", volumeRatio);
        }
    }

    public void addToSumSketch(Tuple tuple, Double volumeRatio) throws NoSuchAlgorithmException {
        String tupleClusterId = tuple.getStringByField("clusterId");
        String tupleStreamId = tuple.getStringByField("streamId");
        String aggregateField = this.aggregatableFields.get(0); // TODO do this with one sketch per aggregate field

        // count_min_sum(C1_S1) += S1.velocity for query SUM(S1.velocity)
        this.panakosSumSketch.add(tupleClusterId + "_" + tupleStreamId, volumeRatio * tuple.getDoubleByField(aggregateField));

        if (tuple.getBooleanByField("isReplica")) {
            this.panakosSumSketch.add(tupleClusterId + "_" + tupleStreamId + "_replica", volumeRatio * tuple.getDoubleByField(aggregateField));
        }
    }

    public Double approxJoinCount(Tuple tuple, Double volumeRatio) throws NoSuchAlgorithmException {
        String tupleClusterId = tuple.getStringByField("clusterId");
        String tupleStreamId = tuple.getStringByField("streamId");

        // query1 = JOIN(S1 x S2 x S3)
        // join_count_C1_query1 = count_min(C1_S1) x count_min(C1_S2) x count_min(C1_S3)
        // so for incoming tuple T1_S1 in cell C1, joining with streams S2 and S3 - join_count = count_min(C1_S2) x count_min(C2_S3)
        Double tupleApproxJoinCount = volumeRatio;
        for (String streamId : this.streamIds) {
            // join this tuple with other streams in this cell
            if (!streamId.equals(tupleStreamId)) {
                tupleApproxJoinCount *= this.panakosCountSketch.query(tupleClusterId + "_" + streamId);
            }
        }

        // if incoming tuple is replica remove join combinations where all tuples are replicas
        if (tuple.getBooleanByField("isReplica")) {
            Double allReplicaCombinationCount = 1.0;
            for (String streamId : this.streamIds) {
                // join this tuple with other streams in this cell
                if (!streamId.equals(tupleStreamId)) {
                    allReplicaCombinationCount *= this.panakosCountSketch.query(tupleClusterId + "_" + streamId + "_replica");
                }
            }

            tupleApproxJoinCount -= allReplicaCombinationCount;
        }

        return tupleApproxJoinCount;
    }

    public Double approxJoinSum(Tuple tuple, Double tupleApproxJoinCount) throws NoSuchAlgorithmException {
        String tupleClusterId = tuple.getStringByField("clusterId");
        double tupleApproxJoinSum = 0;

        // no point computing sum if no join tuples
        if (tupleApproxJoinCount != 0 && this.panakosCountSketch.query(tupleClusterId + "_" + this.aggregateStream) != 0) {
            // query1 = SUM(S1.value)
            // join_sum_C1_query1 = [ join_count_C1_query1 / count_min(C1_S1) ] x count_min_sum(C1_S1)
            tupleApproxJoinSum = (tupleApproxJoinCount / this.panakosCountSketch.query(tupleClusterId + "_" + this.aggregateStream))
                    * this.panakosSumSketch.query(tupleClusterId + "_" + this.aggregateStream);
        }
        
        return tupleApproxJoinSum;
    }
    
    public Double aggregateJoinCounts() {
        return this.clusterJoinCountMap.values().stream().mapToDouble(Double::doubleValue).sum();
    }

    public Double aggregateJoinSums() {
        return this.clusterJoinSumMap.values().stream().mapToDouble(Double::doubleValue).sum();
    }

    public Map<String, Double> getClusterJoinCountMap() {
        return this.clusterJoinCountMap;
    }

    public Map<String, Double> getClusterJoinSumMap() {
        return this.clusterJoinSumMap;
    }   
    
    /////////////////////////////////////////


    public Panakos getPanakosCountSketch() {
        return this.panakosCountSketch;
    }

    public Panakos getPanakosSumSketch() {
        return this.panakosSumSketch;
    }
    
    public void setAggregateStream(String aggregateStream) {
        this.aggregateStream = aggregateStream;
    }

    public void setAggregatableFields(List<String> aggregatableFields) {
        this.aggregatableFields = aggregatableFields;
    }

    public String getAggregateStream() {
        return this.aggregateStream;
    }

    public List<String> getAggregatableFields() {
        return this.aggregatableFields;
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

    public Boolean isTupleEnclosedByClusterForQueryRadius(Tuple tuple) {
        return tuple.getStringByField("enclosedBy").contains(this.id);
    }

    public Boolean isTupleIntersectedByClusterForQueryRadius(Tuple tuple) {
        return tuple.getStringByField("intersectedBy").contains(this.id);
    }

    private List<Tuple> findJoinPartnersInStreamNoIndex(Tuple tuple, QueryGroup queryGroup, String streamToJoin, List<Tuple> window, Boolean calculateDistance) {
        TupleWrapper tupleWrapper = new TupleWrapper(queryGroup.getAxisNamesSorted());
        List<Tuple> joinCandidatesFromOtherStreams = new ArrayList<>();
        for (Tuple joinCandidate : window) {
            if (streamToJoin.equals(joinCandidate.getStringByField("streamId"))) {
                
                // use this to joins tuples that are completely inside the cluster wrt the query raidus
                // so no need to compute distances for these joins, just make sure that join partners are not in the same stream and are also completely enclosed inside the cluster wrt the query radius
                // such join partners can be both replicas and non replicas - they just need to be enclosed by the (same) cell for this query radius
                if (!calculateDistance) {
                    if (this.isTupleEnclosedByClusterForQueryRadius(joinCandidate) &&
                        joinCandidate.getStringByField("clusterId").equals(tuple.getStringByField("clusterId"))) {

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
        BPlusTree<Double, Tuple> bPlusTree = queryGroup.getBPlusTree();
        TupleWrapper tupleWrapper = new TupleWrapper(queryGroup.getAxisNamesSorted());
        List<Tuple> joinCandidates = new ArrayList<>();

        // only need to search for join partners within a cluster as everything joinable is already inside it
        Cluster cluster = queryGroup.getCluster(tuple.getStringByField("clusterId"));
        IDistance iDistance = queryGroup.getIDistance();
        List<Double> searchBounds = iDistance.getSearchBounds(cluster.getRadius(), cluster.getCentroid(),
                cluster.getI(), queryGroup.getC(), this.getRadius(), tupleWrapper.getCoordinates(tuple, this.distance instanceof CosineDistance));

        if (!Double.isNaN(searchBounds.get(0)) && !Double.isNaN(searchBounds.get(1))) {
            joinCandidates.addAll(bPlusTree.searchRange(searchBounds.get(0), BPlusTree.RangePolicy.INCLUSIVE, searchBounds.get(1), BPlusTree.RangePolicy.INCLUSIVE));
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

            // a level contains multiple join combinations wich will now be joined with the next stream 'j'
            for (int i = 0; i < levelSize; i++) {

                List<Tuple> joinCombination = joinCombinations.poll(); // remove [a1, b1] from [ [a1, b1], [a1, b2] ]
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
                    joinCombinations.offer(extendedJoinCombination); // [ [a1, b2], [a1, b1, c1] ]
                }

                // we polled [a1, b1] and then [a2, b2] from joinCombinations but found not partners in stream c to add back to joinCombinations
                // so join joinCombinations is empty
                // return early with empty result
                if (joinCombinations.isEmpty()) {
                    return resultCombinations;
                }
            }
            
            // we just joined the last stream so add the join combinations
            if (j == streamsToJoin.size() - 1) {
                resultCombinations.addAll(joinCombinations);
            }

            j++; // done with this stream, join next stream
        }

        return resultCombinations;
    }
}
