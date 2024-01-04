package aqp;

public class Grid {
  int cellLength;
  int maxJoinRadius;
  List<String> axisNames;
  List<JoinQuery> joinQueries;
  Set<String> streamIds;
  List<Cluster> clusters;
  BPlusTree bPlusTree;

  public Grid(List<String> axisNames) {
    this.cellLength = 0;
    this.maxJoinRadius = 0;
    this.axisNames = axisNames;
    this.streamIds = new HashSet<>();
  }

  public int getCellLength() {
    return this.cellLength;
  }

  public void registerJoinQuery(JoinQuery joinQuery) {
    this.joinQueries.add(joinQuery);
    
    if (joinQuery.getRadius() > this.maxJoinRadius) {
      this.maxJoinRadius = maxJoinRadius;
      this.cellLength = this.maxJoinRadius * 4;
    }

    for (String streamId : joinQuery.getStreamIds()) {
      this.streamIds.add(streamId);
    }
  }

  public List<String> getAxisNames() {
    return Collections.sort(this.axisNames);
  }

  public Set<String> getStreamIds() {
    return Collections.sort(this.streamIds);
  }

  public Boolean isMemberStream(String streamId) {
    return this.streamIds.contains(streamId);
  }

  public void setClusters(List<Cluster> clusters) {
    this.clusters = clusters;
  }

  public void getClusters() {
    return this.clusters;
  }

  public List<JoinQuery> getJoinQueries() {
    return this.joinQueries;
  }

  public BPlusTree getBPlusTree() {
    return this.bPlusTree;
  }

  public void setBplusTree(BPlusTree bPlusTree) {
    this.bPlusTree = bPlusTree;
  }
}
