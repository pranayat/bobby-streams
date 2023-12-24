package aqp;

import java.util.List;

public class EuclideanDistance implements Distance {

    @Override
    public double calculate(List<Double> point1, List<Double> point2) {
        double sum = 0;
        for (int i = 0; i < point1.size(); i++) {
            sum += Math.pow(point1.get(i) - point2.get(i), 2);
        }

        return Math.sqrt(sum);
    }
}
