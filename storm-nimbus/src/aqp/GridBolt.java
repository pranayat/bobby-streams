package aqp;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class GridBolt extends BaseRichBolt {
    OutputCollector _collector;
    private SchemaConfig schemaConfig;

    public GridBolt() {
        this.schemaConfig = SchemaConfigBuilder.build();
    }

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

    private int[] getCube(Tuple tuple, List<String> joinColumns) {
        int dimensions = joinColumns.size();
        int[] cube = new int[dimensions];
        for (int i = 0; i < dimensions; i++) {
            cube[i] = (int) (tuple.getDoubleByField(joinColumns.get(i)) / 10);
        }

        return cube;
    }

    private int[][] getCubesForTuple(Tuple tuple, List<String> joinColumns) {
        int[] cube = getCube(tuple, joinColumns);
        int dimensions = joinColumns.size();
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
            Map stormConfig,
            TopologyContext topologyContext,
            OutputCollector collector) {
        _collector = collector;
//        this.joinQueryCache = JoinQueryCache.getInstance();
    }

    @Override
    public void execute(Tuple input) {
        if (!input.getSourceStreamId().equals("query")) {
//            Set<Map.Entry<UUID, JoinQuery>> querySet = this.joinQueryCache.getAllElements();
            List<String> fields = this.schemaConfig.getStreamById(input.getSourceStreamId());
            List<Object> values;
            // emit tuple to hypercubes of different dimensions (for different join column combinations)
            List<List<String>> joinIndices = this.schemaConfig.getJoinIndices();
            for (List<String> joinColumns : joinIndices) {
                for (int[] cube : this.getCubesForTuple(input, joinColumns)) {
                    System.out.println("emitting to " + Arrays.toString(cube));
                    values = new ArrayList<Object>();
                    for (String field : fields) {
                        values.add(input.getDoubleByField(field));
                    }
                    values.add(Arrays.toString(cube));

                    _collector.emit(input.getSourceStreamId(), new Values(values.toArray()));
                }
            }
        } else {
//            this.joinQueryCache.put(new JoinQuery(input.getStringByField("streamIds").split(","), input.getStringByField("columns").split(","), input.getIntegerByField("distance")));
            _collector.emit(input.getSourceStreamId(), new Values(input.getStringByField("streamIds"), input.getStringByField("columns"), input.getIntegerByField("distance")));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        for (Map.Entry<String, List<String>> stream : this.schemaConfig.getStreams().entrySet()) {
            List<String> fields = new ArrayList<String>(stream.getValue());
            fields.add("cubeId");
            declarer.declareStream(stream.getKey(), new Fields(fields));
        }

        declarer.declareStream("query", new Fields("streamIds", "columns", "distance"));
    }
}
