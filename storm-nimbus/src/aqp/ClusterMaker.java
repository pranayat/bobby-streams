package aqp;

import org.apache.storm.tuple.Tuple;

import java.util.*;

import static java.util.stream.Collectors.toList;

public class ClusterMaker {
    TupleWrapper tupleWrapper;
    Distance distance;
    Random random;

    public ClusterMaker(TupleWrapper tupleWrapper) {
        this.tupleWrapper = tupleWrapper;
        this.distance = new EuclideanDistance();
        this.random = new Random();
    }

    private List<List<Double>> relocateCentroids(Map<List<Double>, List<Tuple>> clusters) {
        return clusters.entrySet().stream().map(e -> average(e.getKey(), e.getValue())).collect(toList());
    }

    private List<Double> average(List<Double> centroid, List<Tuple> tuples) {
        if (tuples == null || tuples.isEmpty()) {
            return centroid;
        }

        int dimensions = this.tupleWrapper.getDimensions();
        List<Double> average = new ArrayList<>(Collections.nCopies(dimensions, 0.0));

        for (Tuple tuple : tuples) {
            List<Double> coordinates = this.tupleWrapper.getCoordinates(tuple);
            for (int i = 0; i < dimensions; i++) {
                average.set(i, average.get(i) + coordinates.get(i));
            }
        }

        average.replaceAll(v -> v / tuples.size());

        return average;
    }

    private void assignToCluster(Map<List<Double>, List<Tuple>> clusters,
                                 Tuple record,
                                 List<Double> centroid) {
        clusters.compute(centroid, (key, list) -> {
            if (list == null) {
                list = new ArrayList<>();
            }

            list.add(record);
            return list;
        });
    }

    private List<List<Double>> randomCentroids(List<Tuple> tuples, int k) {
        List<List<Double>> centroids = new ArrayList<>();
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

            centroids.add(coordinates);
        }

        return centroids;
    }

    private List<Double> nearestCentroid(Tuple tuple, List<List<Double>> centroids) {
        double minimumDistance = Double.MAX_VALUE;
        List<Double> nearest = null;

        for (List<Double> centroid : centroids) {
            double currentDistance = this.distance.calculate(this.tupleWrapper.getCoordinates(tuple), centroid);

            if (currentDistance < minimumDistance) {
                minimumDistance = currentDistance;
                nearest = centroid;
            }
        }

        return nearest;
    }

    public Map<List<Double>, List<Tuple>> fit(List<Tuple> tuples,
                                              int k,
                                              int maxIterations) {

        List<List<Double>> centroids = randomCentroids(tuples, k);
        Map<List<Double>, List<Tuple>> clusters = new LinkedHashMap<>();
        Map<List<Double>, List<Tuple>> lastState = new HashMap<>();

        // iterate for a pre-defined number of times
        for (int i = 0; i < maxIterations; i++) {
            boolean isLastIteration = i == maxIterations - 1;

            // in each iteration we should find the nearest centroid for each record
            for (Tuple tuple : tuples) {
                if (this.tupleWrapper.isQuery(tuple)) {
                    continue;
                }
                List<Double> centroid = nearestCentroid(tuple, centroids);
                assignToCluster(clusters, tuple, centroid);
            }

            // if the assignments do not change, then the algorithm terminates
            boolean shouldTerminate = isLastIteration || clusters.equals(lastState);
            lastState = clusters;
            if (shouldTerminate) {
                break;
            }

            // at the end of each iteration we should relocate the centroids
            centroids = relocateCentroids(clusters);
            clusters = new HashMap<>();
        }

        return lastState;
    }
}
