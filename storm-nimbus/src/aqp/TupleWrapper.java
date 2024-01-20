package aqp;

import org.apache.storm.tuple.Tuple;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class TupleWrapper {
    List<String> coordinateColumns;

    public TupleWrapper(List<String> coordinateColumns) {
        this.coordinateColumns = coordinateColumns;
    }

    public List<Double> getCoordinates(Tuple tuple) {
        return this.getCoordinates(tuple, false);
    }

    public List<Double> getCoordinates(Tuple tuple, Boolean normalize) {
        List<Double> coordinates = new ArrayList<>();
        for (String column : coordinateColumns) {
            coordinates.add(tuple.getDoubleByField(column));
        }

        if (normalize) {
            double magnitude = Math.sqrt(coordinates.stream().mapToDouble(x -> x * x).sum());
            return coordinates.stream().map(x -> x / magnitude).collect(Collectors.toList());
        }

        return coordinates;
    }

    public int getDimensions() {
        return this.coordinateColumns.size();
    }
}
