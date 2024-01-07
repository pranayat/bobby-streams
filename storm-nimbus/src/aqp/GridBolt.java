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
    List<JoinQuery> joinQueries;
    List<Grid> grids;

    public GridBolt() {
        this.schemaConfig = SchemaConfigBuilder.build();
    }

    @Override
    public void prepare(
            Map stormConfig,
            TopologyContext topologyContext,
            OutputCollector collector) {
        _collector = collector;

        this.joinQueries = JoinQueryBuilder.build(this.schemaConfig);
        this.grids = GridBuilder.build(this.joinQueries);
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

    private int[] getCube(Tuple tuple, List<String> joinColumns, int cellSize) {
        int dimensions = joinColumns.size();

        int[] cube = new int[dimensions];
        for (int i = 0; i < dimensions; i++) {
            cube[i] = (int) (tuple.getDoubleByField(joinColumns.get(i)) / cellSize);
        }

        return cube;
    }

    private int[][] getCubesForTuple(Tuple tuple, List<String> joinColumns, int cellSize) {
        int[] cube = getCube(tuple, joinColumns, cellSize);
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
    public void execute(Tuple input) {
        List<Object> values;
        String tupleStreamId = input.getSourceStreamId();
        Stream tupleStream = this.schemaConfig.getStreamById(tupleStreamId);


        for (Grid grid : this.grids) {
            if (!grid.isMemberStream(tupleStreamId)) {
                continue;
            }

            for (int[] cube : this.getCubesForTuple(input, grid.getAxisNamesSorted(), grid.getCellLength())) {
                System.out.println("emitting to " + Arrays.toString(cube));
                values = new ArrayList<Object>();
                for (Field field : tupleStream.getFields()) {
                    if (field.getType().equals("double")) {
                        values.add(input.getDoubleByField(field.getName()));
                    } else {
                        values.add(input.getStringByField(field.getName()));
                    }
                }

                values.add(Arrays.toString(cube));
                values.add(grid.getName());

                _collector.emit(tupleStreamId, new Values(values.toArray()));
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        for (Stream stream : this.schemaConfig.getStreams()) {
            List<String> fields = new ArrayList<String>(stream.getFieldNames());
            fields.add("cubeId");
            fields.add("gridName");
            declarer.declareStream(stream.getId(), new Fields(fields));
        }
    }
}
