package aqp;

import java.util.Arrays;
import java.util.UUID;

public class JoinQuery {
    private final UUID id;
    private String[] streamIds;
    private String[] columns;
    private int distance;

    public JoinQuery(String[] streamIds, String[] columns, int distance) {
        this.id = UUID.randomUUID();
        this.streamIds = streamIds;
        this.columns = columns;
        Arrays.sort(this.columns, String.CASE_INSENSITIVE_ORDER);
        this.distance = distance;
    }

    public UUID getId() {
        return id;
    }

    public String[] getStreamIds() {
        return streamIds;
    }

    public void setStreamIds(String[] streamIds) {
        this.streamIds = streamIds;
    }

    public String[] getColumns() {
        return columns;
    }

    public void setColumns(String[] columns) {
        this.columns = columns;
    }

    public int getDistance() {
        return distance;
    }

    public void setDistance(int distance) {
        this.distance = distance;
    }

    public int getDimension() {
        return this.columns.length;
    }
}
