package aqp;

import java.util.List;

public class CosineDistance implements Distance {

    @Override
    public double calculate(List<Double> point1, List<Double> point2) {
        double dotProduct = 0.0;
        double normPoint1 = 0.0;
        double normPoint2 = 0.0;
        for (int i = 0; i < point1.size(); i++) {
            dotProduct += point1.get(i) * point2.get(i);
            normPoint1 += Math.pow(point1.get(i), 2);
            normPoint2 += Math.pow(point2.get(i), 2);
        }

        return 1 - (dotProduct / (Math.sqrt(normPoint1) * Math.sqrt(normPoint2)));
    }
}
