package aqp;

import java.util.*;

public class Grid {
    int cellLength;
    int maxJoinRadius;
    List<String> axisNames;
    List<JoinQuery> joinQueries;
    Set<String> streamIds;
    List<Cluster> clusters;
    BPlusTree bPlusTree;
    String name;
    int c;

    public Grid(List<String> axisNames) {
        this.cellLength = 0;
        this.maxJoinRadius = 0;
        this.c = 10000;
        this.axisNames = axisNames;
        Collections.sort(this.axisNames);
        this.name = this.axisNames.toString();
        this.streamIds = new HashSet<>();
        this.joinQueries = new ArrayList<>();
    }

    public int getCellLength() {
        return this.cellLength;
    }

    public int getC() {
        return this.c;
    }
    
    public void registerJoinQuery(JoinQuery joinQuery) {
        this.joinQueries.add(joinQuery);

        if (joinQuery.getRadius() > this.maxJoinRadius) {
            this.maxJoinRadius = joinQuery.getRadius();
            this.cellLength = this.maxJoinRadius * 4;
        }

        for (String streamId : joinQuery.getStreamIds()) {
            this.streamIds.add(streamId);
        }
    }

    public List<String> getAxisNamesSorted() {
        Collections.sort(this.axisNames);
        return this.axisNames;
    }

    public Set<String> getStreamIds() {
        return this.streamIds;
    }

    public Boolean isMemberStream(String streamId) {
        return this.streamIds.contains(streamId);
    }

    public void setClusters(List<Cluster> clusters) {
        this.clusters = clusters;
    }

    public List<Cluster> getClusters() {
        return this.clusters;
    }

    public List<JoinQuery> getJoinQueries() {
        return this.joinQueries;
    }

    public BPlusTree getBPlusTree() {
        return this.bPlusTree;
    }

    public void setBPlusTree(BPlusTree bPlusTree) {
        this.bPlusTree = bPlusTree;
    }

    public String getName() {
        return this.name;
    }
}
