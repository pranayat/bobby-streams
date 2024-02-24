package aqp;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class GridCellAssignerBolt extends BaseRichBolt {
    OutputCollector _collector;
    private SchemaConfig schemaConfig;
    List<JoinQuery> joinQueries;
    List<QueryGroup> queryGroups;

    public GridCellAssignerBolt() {
        this.schemaConfig = SchemaConfigBuilder.build();
    }

    @Override
    public void prepare(
            Map stormConfig,
            TopologyContext topologyContext,
            OutputCollector collector) {
        _collector = collector;

        this.joinQueries = JoinQueryBuilder.build(this.schemaConfig);
        this.queryGroups = QueryGroupBuilder.build(this.joinQueries);
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

    private int[] getCube(Tuple tuple, List<String> joinColumns, int cellLength) {
        int dimensions = joinColumns.size();

        int[] cube = new int[dimensions];
        for (int i = 0; i < dimensions; i++) {
            cube[i] = (int) (tuple.getDoubleByField(joinColumns.get(i)) / cellLength);
        }

        return cube;
    }

    private int[][] getCubesForTuple(Tuple tuple, List<String> joinColumns, int cellLength) {
        int[] cube = getCube(tuple, joinColumns, cellLength);
        int dimensions = joinColumns.size();
        // TODO handle replication in partition bolt, replicate only if clusters are in partition bolt and being assigned to different partitions
//        int adjacentCount = (int) Math.pow(3, dimensions);
//        int[][] allCubes = new int[adjacentCount + 1][dimensions];

//        int[] adjacentCube;
//        for (int i = 0; i < adjacentCount; i++) {
//            adjacentCube = this.calculateAdjacentCube(cube, i, dimensions);
//            allCubes[i] = adjacentCube;
//        }
//
//        allCubes[adjacentCount] = cube;
//        return allCubes;
        int[][] allCubes = new int[1][dimensions];
        allCubes[0] = cube;
        return allCubes;
    }

    @Override
    public void execute(Tuple input) {
        List<Object> values;
        String tupleStreamId = input.getSourceStreamId();
        Stream tupleStream = this.schemaConfig.getStreamById(tupleStreamId);


        for (QueryGroup queryGroup : this.queryGroups) {
            // the tuple's stream is not relevant for this set of queries
            if (!queryGroup.isMemberStream(tupleStreamId)) {
                continue;
            }

            // multiple cubes as it is replicated into adjacent cubes as well
            // each cube is an int array [2,2]
            for (int[] cubeLabel : this.getCubesForTuple(input, queryGroup.getAxisNamesSorted(), queryGroup.getCellLength())) {
//                System.out.println("emitting to " + Arrays.toString(cube));
                values = new ArrayList<Object>();
                for (Field field : tupleStream.getFields()) {
                    if (field.getType().equals("double")) {
                        values.add(input.getDoubleByField(field.getName()));
                    } else {
                        values.add(input.getStringByField(field.getName()));
                    }
                }

                TupleWrapper tupleWrapper = new TupleWrapper(queryGroup.getAxisNamesSorted());
                List<Double> coordinates = tupleWrapper.getCoordinates(input);
                List<Double> centroid = new ArrayList<>();
                // for cube [2,3] cell length 10 with centroid [2*10 + 10/2, 3*10 + 10/2 ][25, 35]
                for (int axisOffset : cubeLabel) {
                    centroid.add((double) (axisOffset * queryGroup.getCellLength() + queryGroup.getCellLength() / 2));
                }

                values.add(centroid.toString()); // cluster Id eg. (25, 35)
                values.add(queryGroup.getName()); // query group Id eg. (lat, long)

                // stream_1, (25,35) (lat,long) ...
                _collector.emit(tupleStreamId, input, new Values(values.toArray()));
            }
        }
        _collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        for (Stream stream : this.schemaConfig.getStreams()) {
            List<String> fields = new ArrayList<String>(stream.getFieldNames());
            fields.add("clusterId"); // centroid of cube in this case
            fields.add("queryGroupName");
            declarer.declareStream(stream.getId(), new Fields(fields));
        }
    }
}
