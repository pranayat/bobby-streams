package aqp;

import org.apache.storm.tuple.Tuple;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

public class Cluster {
    List<Double> centroid;
    List<Tuple> tuples;
    double radius;
    Distance distance;
    TupleWrapper tupleWrapper;
    static int counter = 0;
    int i;

    Cluster(List<Double> centroid, TupleWrapper tupleWrapper) {
        this.centroid = centroid;
        this.tuples = new ArrayList<>();
        this.radius = 0;
        this.distance = new EuclideanDistance();
        this.tupleWrapper = tupleWrapper;
        counter += 1;
        this.i = counter;
    }

    public int getI() {
        return this.i;
    }

    public double getRadius() {
        return this.radius;
    }

    public List<Double> getCentroid() {
        return this.centroid;
    }

    public void addTuple(Tuple tuple) {
        this.tuples.add(tuple);
        this.expandRadiusWithTuple(tuple);
    }

    public void expandRadiusWithTuple(Tuple tuple) {
        double distance = this.distance.calculate(centroid, this.tupleWrapper.getCoordinates(tuple));
        if (distance > this.radius) {
            this.radius = distance;
        }
    }

    public List<Tuple> getTuples() {
        return this.tuples;
    }

    public int getTupleCount() {
        return this.tuples.size();
    }

    public void setCentroid(List<Double> centroid) {
        this.centroid = centroid;
    }
}
