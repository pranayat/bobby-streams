package aqp;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class SchemaConfig implements Serializable {
    @JsonProperty("streams")
    private Map<String, List<String>> streams;

    @JsonProperty("join_indices")
    private List<List<String>> joinIndices;

    // Getters
    public Map<String, List<String>> getStreams() {
        return streams;
    }

    public List<String> getStreamById(String streamId) {
        return streams.get(streamId);
    }

    public List<List<String>> getJoinIndices() {
        return joinIndices;
    }

    // Setters
    public void setStreams(Map<String, List<String>> streams) {
        this.streams = streams;
    }

    public void setJoinIndices(List<List<String>> joinIndices) {
        this.joinIndices = joinIndices;
    }

    @Override
    public String toString() {
        return "SchemaConfig{" +
                "streams=" + streams +
                ", joinIndices=" + joinIndices +
                '}';
    }
}
