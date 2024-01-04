package aqp;

import org.apache.storm.tuple.Tuple;

public class JoinQuery {
  int radius;
  List<String> streamIds;
  List<String> fields;
  List<List<Tuple>> results;

  public JoinQuery(int radius, List<String> streamIds, List<String> fields) {
    this.radius = radius;
    this.streamIds = streamIds;
    this.fields = fields;
    this.results = new ArrayList<>();
  }

  public int getRadius() {
    return this.radius;
  }

    public int getStreamIds() {
    return this.streamIds;
  }

    public int getFields() {
    return Collections.sort(this.fields);
  }

  public void addResult(List<Tuple> result) {
    this.results.add(result);
  }
}
