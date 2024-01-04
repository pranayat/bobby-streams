package aqp;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class SchemaConfig {
    
    @JsonProperty("streams")
    private List<Stream> streams;
    
    @JsonProperty("queries")
    private List<Query> queries;

    // Add getters and setters

    public List<Stream> getStreams() {
        return streams;
    }

    public Stream getStreamById(String streamId) {
        Stream foundStream = streams.stream()
                .filter(stream -> stream.getId().equals(streamId))
                .findFirst()
                .get();
    }    

    public void setStreams(List<Stream> streams) {
        this.streams = streams;
    }

    public List<Query> getQueries() {
        return queries;
    }

    public void setQueries(List<Query> queries) {
        this.queries = queries;
    }
}

class Stream {
    
    @JsonProperty("id")
    private String id;
    
    @JsonProperty("fields")
    private List<String> fields;

    // Add getters and setters

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public List<String> getFields() {
        return fields;
    }

    public void setFields(List<String> fields) {
        this.fields = fields;
    }
}

class Query {
    
    @JsonProperty("id")
    private String id;
    
    @JsonProperty("stages")
    private List<Stage> stages;

    // Add getters and setters

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public List<Stage> getStages() {
        return stages;
    }

    public void setStages(List<Stage> stages) {
        this.stages = stages;
    }
}

class Stage {
    
    @JsonProperty("type")
    private String type;
    
    @JsonProperty("between")
    private List<String> between;
    
    @JsonProperty("on")
    private List<String> on;
    
    @JsonProperty("max_distance")
    private int maxDistance;

    // Add getters and setters

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public List<String> getBetween() {
        return between;
    }

    public void setBetween(List<String> between) {
        this.between = between;
    }

    public List<String> getOn() {
        return on;
    }

    public void setOn(List<String> on) {
        this.on = on;
    }

    public int getMaxDistance() {
        return maxDistance;
    }

    public void setMaxDistance(int maxDistance) {
        this.maxDistance = maxDistance;
    }
}
