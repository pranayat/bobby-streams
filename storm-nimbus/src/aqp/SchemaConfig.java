package aqp;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;

public class SchemaConfig implements Serializable {

    @JsonProperty("streams")
    private List<Stream> streams;

    @JsonProperty("queries")
    private List<Query> queries;

    // Add getters and setters

    public List<Stream> getStreams() {
        return streams;
    }

    public Stream getStreamById(String streamId) {
        return streams.stream()
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

class Stream implements Serializable {

    @JsonProperty("id")
    private String id;

    @JsonProperty("fields")
    private List<Field> fields;

    // Add getters and setters

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public List<Field> getFields() {
        return fields;
    }

    public List<String> getFieldNames() {
        return this.fields.stream()
                .map(Field::getName)
                .collect(Collectors.toList());
    }

    public void setFields(List<Field> fields) {
        this.fields = fields;
    }
}

class Field implements Serializable {
    @JsonProperty("name")
    private String name;

    @JsonProperty("type")
    private String type;

    public String getName() {
        return this.name;
    }

    public String getType() {
        return this.type;
    }
}

class Query implements Serializable {

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

class Stage implements Serializable {

    @JsonProperty("type")
    private String type;

    @JsonProperty("between")
    private List<String> between;

    @JsonProperty("on")
    private List<String> on;

    @JsonProperty("radius")
    private double radius;

    @JsonProperty("distanceType")
    private String distanceType;

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

    public double getRadius() {
        return radius;
    }

    public void setRadius(double radius) {
        this.radius = radius;
    }

    public void setDistanceType(String distanceType) {
        this.distanceType = distanceType;
    }

    public String getDistanceType() {
        return this.distanceType;
    }
}
