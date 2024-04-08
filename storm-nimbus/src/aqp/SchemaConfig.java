package aqp;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class SchemaConfig implements Serializable {

    private static final long serialVersionUID = 1234567L;

    @JsonProperty("streams")
    private List<Stream> streams;

    @JsonProperty("queries")
    private List<Query> queries;

    @JsonProperty("clustering")
    private Clustering clustering;

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

    public Clustering getClustering() {
        return this.clustering;
    }

    public void setQueries(List<Query> queries) {
        this.queries = queries;
    }
}

class Stream implements Serializable {

    private static final long serialVersionUID = 1234567L;

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

    private static final long serialVersionUID = 1234567L;

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

class Clustering implements Serializable {

    private static final long serialVersionUID = 1234567L;

    @JsonProperty("type")
    private String type;

    @JsonProperty("k")
    private int k;

    @JsonProperty("iterations")
    private int iterations;

    public String getType() {
        return this.type;
    }

    public int getK() {
        return this.k;
    }

    public int getIterations() {
        return this.iterations;
    }
}

class Query implements Serializable {

    private static final long serialVersionUID = 1234567L;

    private List<String> supportedStages;

    public Query() {
        this.supportedStages = new ArrayList<>();
        supportedStages.add("count");
        supportedStages.add("sum");
        supportedStages.add("avg");
    }

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

    public Stage getAggregationStage() {
        for (Stage stage : this.stages) {
            if (this.supportedStages.contains(stage.getType())) {
                return stage;
            }
        }

        return null;
    }
}

class Stage implements Serializable {

    private static final long serialVersionUID = 1234567L;

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

    @JsonProperty("aggregateStream")
    private String aggregateStream;

    @JsonProperty("aggregatableFields")
    private List<String> aggregatableFields;

    @JsonProperty("clauses")
    private List<Clause> clauses;

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

    public String getAggregateStream() {
        return this.aggregateStream;
    }    

    public List<String> getAggregatableFields() {
        return this.aggregatableFields;
    }

    public List<Clause> getClauses() {
        return this.clauses;
    }

    public void setClauses(List<Clause> clauses) {
        this.clauses = clauses;
    }
}

class Clause implements Serializable {

    private static final long serialVersionUID = 1234567L;

    @JsonProperty("stream")
    private String stream;

    @JsonProperty("field")
    private String field;

    @JsonProperty("operator")
    private String operator;

    @JsonProperty("value")
    private String value;

    public String getStream() {
        return this.stream;
    }

    public void setStream(String stream) {
        this.stream = stream;
    }

    public String getField() {
        return this.field;
    }

    public void setField(String field) {
        this.field = field;
    }

    public String getOperator() {
        return this.operator;
    }

    public void setOperator(String operator) {
        this.operator = operator;
    }

    public String getValue() {
        return this.value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}