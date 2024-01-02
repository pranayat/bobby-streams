package aqp;

import org.apache.storm.tuple.Tuple;

import java.util.*;

class Cluster {
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

public interface ClusterMaker {
    public List<Cluster> fit(List<Tuple> tuples);
}

class GridClusterMaker implements ClusterMaker {
    TupleWrapper tupleWrapper;
    int cellSize;

    public GridClusterMaker(TupleWrapper tupleWrapper, int cellSize) {
        this.tupleWrapper = tupleWrapper;
        this.cellSize = cellSize;
    }

    public List<Cluster> fit(List<Tuple> tuples) {
        List<Cluster> clusters = new ArrayList<>();
        Map<String, Cluster> clusterMap = new HashMap<>();

        for (Tuple tuple : tuples) {
            String cubeId = tuple.getStringByField("cubeId");
            Cluster cluster = clusterMap.get(cubeId);

            if (cluster != null) {
                cluster.addTuple(tuple);
            } else {
                // TODO do this in gridbolt when computing cubeId ?
                List<Double> coordinates = this.tupleWrapper.getCoordinates(tuple);
                List<Double> centroid = new ArrayList<>();
                for (Double coordinate : coordinates) {
                    centroid.add((double) ((int) (coordinate / this.cellSize)) * this.cellSize + (this.cellSize / 2));
                }
                cluster = new Cluster(centroid, this.tupleWrapper);
                cluster.addTuple(tuple);
                clusterMap.put(cubeId, cluster);
            }
        }

        return new ArrayList<Cluster>(clusterMap.values());
    }
}

class KMeansClusterMaker implements ClusterMaker {
    TupleWrapper tupleWrapper;
    int k;
    int maxIterations;
    Distance distance;
    Random random;

    public KMeansClusterMaker(TupleWrapper tupleWrapper, int k, int maxIterations) {
        this.tupleWrapper = tupleWrapper;
        this.k = k;
        this.maxIterations = maxIterations;
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
        List<Double> maxs = new ArrayList<>(Collections.nCopies(dimensions, null));
        List<Double> mins = new ArrayList<>(Collections.nCopies(dimensions, null));
        List<Double> coordinates;

        for (Tuple tuple : tuples) {
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

        assert nearest != null;
        nearest.addTuple(tuple);
    }

    public List<Cluster> fit(List<Tuple> tuples) {

        List<Cluster> clusters = randomClusters(tuples, this.k);
        List<Cluster> lastState = new ArrayList<>();

        // iterate for a pre-defined number of times
        for (int i = 0; i < this.maxIterations; i++) {
            boolean isLastIteration = i == this.maxIterations - 1;

            // in each iteration we should find the nearest centroid for each record
            for (Tuple tuple : tuples) {
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
