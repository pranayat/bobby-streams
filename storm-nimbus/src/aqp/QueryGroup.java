package aqp;

import java.util.*;
import org.apache.storm.tuple.Tuple;
// A query group is identified by its the query axes eg. lat, long
// Each query group has is responsible for creating clusters be they grid cells or k means clusters using the
// query groups axes
// All queries that have these axes can use this query group
// Eg. query 1 joining stream 1 and stream 2 using lat and long
// and query 2 joining stream 1, stream 2, stream 3 using lat and long
// this means the clusters associated with this query group should have tuples belonging to streams 1, 2 and 3
// and the clustering should be done using lat and long
public class QueryGroup {
    int cellLength; // TODO change to double
    double maxJoinRadius;
    double minJoinRadius;
    List<String> axisNames;
    List<JoinQuery> joinQueries;
    Set<String> streamIds;
    Map<String, Cluster> clusterMap;
    BPlusTreeNew<Double, Tuple> bPlusTree;
    String name;
    int c;
    Distance distance;
    IDistance iDistance;

    public QueryGroup(List<String> axisNames, Distance distance, IDistance iDistance) {
        this.cellLength = 0;
        this.maxJoinRadius = 0;
        this.minJoinRadius = Double.POSITIVE_INFINITY;
        this.c = 1_000_000;
        this.axisNames = axisNames;
        Collections.sort(this.axisNames);
        this.name = this.axisNames.toString() + "-" + distance.getClass().getSimpleName();
        this.streamIds = new HashSet<>();
        this.joinQueries = new ArrayList<>();
        this.clusterMap = new HashMap<>();
        this.bPlusTree = new BPlusTreeNew<Double, Tuple>(128);
        this.distance = distance;
        this.iDistance = iDistance;
    }

    public int getCellLength() {
        return this.cellLength;
    }

    public int getC() {
        return this.c;
    }

    public void registerJoinQuery(JoinQuery joinQuery) {
        this.joinQueries.add(joinQuery);

        if (joinQuery.getDistance() instanceof EuclideanDistance && joinQuery.getRadius() < this.minJoinRadius) {
            this.minJoinRadius = joinQuery.getRadius();
            this.cellLength = (int) (this.minJoinRadius / Math.sqrt(2));
        }

        if (joinQuery.getDistance() instanceof EuclideanDistance && joinQuery.getRadius() > this.maxJoinRadius) {
            this.maxJoinRadius = joinQuery.getRadius();
        }

        // if there were no euclidean queries but only cosine ones then use 10_000 as default
        if (this.cellLength == 0) {
            this.cellLength = 10_000;
        }

        for (String streamId : joinQuery.getStreamIds()) {
            this.streamIds.add(streamId);
        }
    }

    public List<String> getAxisNamesSorted() {
        Collections.sort(this.axisNames);
        return this.axisNames;
    }

    public Distance getDistance() {
        return this.distance;
    }

    public IDistance getIDistance() {
        return this.iDistance;
    }

    public Set<String> getStreamIds() {
        return this.streamIds;
    }

    public Boolean isMemberStream(String streamId) {
        return this.streamIds.contains(streamId);
    }

    public Map<String, Cluster> getClusterMap() {
        return this.clusterMap;
    }

    public Cluster getCluster(String clusterId) {
        return this.clusterMap.get(clusterId);
    }

    public void setCluster(String clusterId, Cluster cluster) {
        this.clusterMap.put(clusterId, cluster);
    }

    public List<JoinQuery> getJoinQueries() {
        return this.joinQueries;
    }

    public BPlusTreeNew<Double, Tuple> getBPlusTree() {
        return this.bPlusTree;
    }

    public void setBPlusTree(BPlusTreeNew<Double, Tuple> bPlusTree) {
        this.bPlusTree = bPlusTree;
    }

    public String getName() {
        return this.name;
    }
}
