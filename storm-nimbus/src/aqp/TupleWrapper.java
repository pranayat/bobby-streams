package aqp;

import org.apache.storm.tuple.Tuple;

import java.util.ArrayList;
import java.util.List;

public class TupleWrapper {
    List<String> coordinateColumns;

    public TupleWrapper(List<String> coordinateColumns) {
        this.coordinateColumns = coordinateColumns;
    }

    public List<Double> getCoordinates(Tuple tuple) {
        List<Double> coordinates = new ArrayList<>();
        for (String column : coordinateColumns) {
            coordinates.add(tuple.getDoubleByField(column));
        }

        return coordinates;
    }

    public int getDimensions() {
        return this.coordinateColumns.size();
    }
}
