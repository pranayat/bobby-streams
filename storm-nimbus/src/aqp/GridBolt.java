package aqp;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public class GridBolt extends BaseRichBolt {
    OutputCollector _collector;
    private JoinQueryCache joinQueryCache;

    private int[] calculateAdjacentCube(int[] cubeLabel, int index, int dimensions) {
        int[] adjacentCube = new int[dimensions];

        // Loop through each dimension
        for (int j = 0; j < dimensions; j++) {
            // Calculate the offset for the current dimension using ternary representation
            int offset = (index % 3) - 1;
            // Calculate the label of the adjacent cube for the current dimension
            adjacentCube[j] = cubeLabel[j] + offset;
            // Move to the next ternary digit
            index /= 3;
        }

        return adjacentCube;
    }

    private int[] getCube(Tuple tuple, JoinQuery joinQuery) {
        int[] cube = new int[joinQuery.getDimension()];

        String[] columns = joinQuery.getColumns();
        int dimensions = joinQuery.getDimension();
        for (int i = 0; i < dimensions; i++) {
            cube[i] = (int) (tuple.getDoubleByField(columns[i]) / 10);
        }

        return cube;
    }

    private int[][] getCubesForTuple(Tuple tuple, JoinQuery joinQuery) {
        int[] cube = getCube(tuple, joinQuery);
        int dimensions = joinQuery.getDimension();
        int adjacentCount = (int) Math.pow(3, dimensions);
        int[][] allCubes = new int[adjacentCount + 1][dimensions];

        int[] adjacentCube;
        for (int i = 0; i < adjacentCount; i++) {
            adjacentCube = this.calculateAdjacentCube(cube, i, dimensions);
            allCubes[i] = adjacentCube;
        }

        allCubes[adjacentCount] = cube;
        return allCubes;
    }

    @Override
    public void prepare(
            Map map,
            TopologyContext topologyContext,
            OutputCollector collector) {
        _collector = collector;

        this.joinQueryCache = JoinQueryCache.getInstance();
    }

    @Override
    public void execute(Tuple input) {
        if (input.getSourceComponent().equals("data")) {
            if (this.joinQueryCache.size() == 0) {
                // which cube to emit to ?
                return;
            }
            Set<Map.Entry<UUID, JoinQuery>> querySet = this.joinQueryCache.getAllElements();
            for (Map.Entry<UUID, JoinQuery> joinQuery : querySet) {
                for (int[] cube : this.getCubesForTuple(input, joinQuery.getValue())) {
                    System.out.println("emitting to " + Arrays.toString(cube));
                    _collector.emit(new Values("data", "stream_1", Arrays.toString(cube), input.getDoubleByField("lat"), input.getDoubleByField("long"), input.getDoubleByField("alt"), input.getStringByField("text")));
                }
            }
        } else {
            this.joinQueryCache.put(new JoinQuery(input.getStringByField("streamIds").split(","), input.getStringByField("columns").split(","), input.getIntegerByField("distance")));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("tupleType", "streamId", "cubeId", "lat", "long", "alt", "text"));
    }
}
