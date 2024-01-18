package aqp;

import java.util.Arrays;
import java.util.List;

class CosineIDistance extends IDistance {
    Distance distance;

    public CosineIDistance() {
        this.distance = new CosineDistance();
    }

    @Override
    public List<Double> getSearchBounds(double clusterRadius, List<Double> centroid, int i, int c, double joinRadius, List<Double> tupleCoordinates) {
        double queryTupleToCentroidDistance = this.distance.calculate(centroid, tupleCoordinates);

        double sqrtTerm = Math.sqrt((1 - Math.pow(1 - queryTupleToCentroidDistance, 2) * (1 - Math.pow(1 - joinRadius, 2))));
        double mulTerm = (1 - queryTupleToCentroidDistance) * (1 - joinRadius);
        double lowerBound = i * c - sqrtTerm - mulTerm + 1;
        double upperBound;
        switch (this.getQuerySpherePosition(queryTupleToCentroidDistance, clusterRadius, joinRadius)) {
            case INSIDE:
                upperBound = Math.min(i * c + clusterRadius,
                        i * c + sqrtTerm - mulTerm + 1);
                break;
            case INTERSECTS:
                upperBound = i * c + clusterRadius;
                break;
            default:
                lowerBound = Double.NaN;
                upperBound = Double.NaN;
        }

        return Arrays.asList(lowerBound, upperBound);
    }
}

class EuclideanIDistance extends IDistance {
    Distance distance;

    public EuclideanIDistance() {
        this.distance = new EuclideanDistance();
    }

    @Override
    public List<Double> getSearchBounds(double clusterRadius, List<Double> centroid, int i, int c, double joinRadius, List<Double> tupleCoordinates) {
        double queryTupleToCentroidDistance = this.distance.calculate(centroid, tupleCoordinates);

        double lowerBound = i * c + queryTupleToCentroidDistance - joinRadius;
        double upperBound;
        switch (this.getQuerySpherePosition(queryTupleToCentroidDistance, clusterRadius, joinRadius)) {
            case INSIDE:
                upperBound = Math.min(i * c + clusterRadius, i * c + queryTupleToCentroidDistance + joinRadius);
                break;
            case INTERSECTS:
                upperBound = i * c + clusterRadius;
                break;
            default:
                lowerBound = Double.NaN;
                upperBound = Double.NaN;
        }

        return Arrays.asList(lowerBound, upperBound);
    }
}

public abstract class IDistance {
    enum QuerySpherePosition {
        INSIDE,
        INTERSECTS,
        OUTSIDE
    }

    protected QuerySpherePosition getQuerySpherePosition(double queryTupleToCentroidDistance, double clusterRadius, double queryRadius) {
        if (queryTupleToCentroidDistance < clusterRadius) {
            return QuerySpherePosition.INSIDE;
        } else if (queryTupleToCentroidDistance < clusterRadius + queryRadius) {
            return QuerySpherePosition.INTERSECTS;
        } else {
            return QuerySpherePosition.OUTSIDE;
        }
    }

    public abstract List<Double> getSearchBounds(double clusterRadius, List<Double> centroid, int i, int c, double joinRadius, List<Double> tupleCoordinates);
}
