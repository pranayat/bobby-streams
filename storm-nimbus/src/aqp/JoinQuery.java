package aqp;

import org.apache.storm.tuple.Tuple;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class JoinQuery {
    int radius;
    List<String> streamIds;
    List<String> fields;
    List<List<Tuple>> results;

    public JoinQuery(int radius, List<String> streamIds, List<String> fields) {
        Collections.sort(fields);
        this.radius = radius;
        this.streamIds = streamIds;
        this.fields = fields;
        this.results = new ArrayList<>();
    }

    public int getRadius() {
        return this.radius;
    }

    public List<String> getStreamIds() {
        return this.streamIds;
    }

    public List<String> getFieldsSorted() {
        Collections.sort(this.fields);
        return this.fields;
    }

    public void addResult(List<Tuple> result) {
        this.results.add(result);
    }
}
