package aqp;

import org.apache.storm.tuple.Tuple;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

class Cluster {
    List<Double> centroid;
    List<Tuple> tuples;
    double maxDistance;
    Distance distance;
    TupleWrapper tupleWrapper;

    Cluster(List<Double> centroid, TupleWrapper tupleWrapper) {
        this.centroid = centroid;
        this.tuples = new ArrayList<>();
        this.maxDistance = 0;
        this.distance = new EuclideanDistance();
        this.tupleWrapper = tupleWrapper;
    }

    public List<Double> getCentroid() {
        return this.centroid;
    }

    public void addTuple(Tuple tuple) {
        this.tuples.add(tuple);
        double distance = this.distance.calculate(centroid, this.tupleWrapper.getCoordinates(tuple));
        if (distance > this.maxDistance) {
            this.maxDistance = distance;
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

public class ClusterMaker {
    TupleWrapper tupleWrapper;
    Distance distance;
    Random random;

    public ClusterMaker(TupleWrapper tupleWrapper) {
        this.tupleWrapper = tupleWrapper;
        this.distance = new EuclideanDistance();
        this.random = new Random();
    }

    private void recomputeClusterCentroids(List<Cluster> clusters) {
        int dimensions = this.tupleWrapper.getDimensions();
        for (Cluster cluster : clusters) {
            List<Double> newCentroid = new ArrayList<>(Collections.nCopies(dimensions, 0.0));
            for (Tuple tuple : cluster.getTuples()) {
                for (int i = 0; i < dimensions; i++) {
                    newCentroid.set(i, newCentroid.get(i) + this.tupleWrapper.getCoordinates(tuple).get(i));
                }
            }
            newCentroid.replaceAll(v -> v / cluster.getTupleCount());
            cluster.setCentroid(newCentroid);
        }
    }

    private List<Cluster> randomClusters(List<Tuple> tuples, int k) {
        List<Cluster> clusters = new ArrayList<>();
        int dimensions = this.tupleWrapper.getDimensions();
        List<Double> maxs = new ArrayList<>(Collections.nCopies(dimensions, (Double) null));
        List<Double> mins = new ArrayList<>(Collections.nCopies(dimensions, (Double) null));
        List<Double> coordinates = new ArrayList<>();

        for (Tuple tuple : tuples) {
            if (this.tupleWrapper.isQuery(tuple)) {
                continue;
            }

            coordinates = this.tupleWrapper.getCoordinates(tuple);
            for (int i = 0; i < dimensions; i++) {
                maxs.set(i, maxs.get(i) == null || coordinates.get(i) > maxs.get(i) ? coordinates.get(i) : maxs.get(i));
                mins.set(i, mins.get(i) == null || coordinates.get(i) < mins.get(i) ? coordinates.get(i) : mins.get(i));
            }
        }

        for (int i = 0; i < k; i++) {
            coordinates = new ArrayList<>();
            for (int j = 0; j < dimensions; j++) {
                double max = maxs.get(j);
                double min = mins.get(j);
                coordinates.add(this.random.nextDouble() * (max - min) + min);
            }

            clusters.add(new Cluster(coordinates, this.tupleWrapper));
        }

        return clusters;
    }

    private void assignToNearestCluster(Tuple tuple, List<Cluster> clusters) {
        double minimumDistance = Double.MAX_VALUE;
        Cluster nearest = null;

        for (Cluster cluster : clusters) {
            double currentDistance = this.distance.calculate(this.tupleWrapper.getCoordinates(tuple), cluster.getCentroid());

            if (currentDistance < minimumDistance) {
                minimumDistance = currentDistance;
                nearest = cluster;
            }
        }

        nearest.addTuple(tuple);
    }

    public List<Cluster> fit(List<Tuple> tuples,
                             int k,
                             int maxIterations) {

        List<Cluster> clusters = randomClusters(tuples, k);
        List<Cluster> lastState = new ArrayList<>();

        // iterate for a pre-defined number of times
        for (int i = 0; i < maxIterations; i++) {
            boolean isLastIteration = i == maxIterations - 1;

            // in each iteration we should find the nearest centroid for each record
            for (Tuple tuple : tuples) {
                if (this.tupleWrapper.isQuery(tuple)) {
                    continue;
                }
                assignToNearestCluster(tuple, clusters);
            }

            // if the assignments do not change, then the algorithm terminates
            boolean shouldTerminate = isLastIteration || clusters.equals(lastState);
            lastState = clusters;
            if (shouldTerminate) {
                break;
            }

            recomputeClusterCentroids(clusters);
        }

        return lastState;
    }
}
