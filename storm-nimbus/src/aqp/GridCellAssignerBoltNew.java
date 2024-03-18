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
import java.util.Arrays;

public class GridCellAssignerBoltNew extends BaseRichBolt {
    OutputCollector _collector;
    private SchemaConfig schemaConfig;
    List<JoinQuery> joinQueries;
    List<QueryGroup> queryGroups;

    public GridCellAssignerBoltNew() {
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

    private int[] getTupleCell(List<Double> tupleCoordinates, int dimensions, int cellLength) {
      int[] cell = new int[dimensions];
      for (int i = 0; i < dimensions; i++) {
          cell[i] = (int) (Math.floor(tupleCoordinates.get(i) / cellLength));
      }

      return cell;
    }

    private Boolean isValidTargetCell(int[] targetCell, int[] tupleCell) {
      if (Arrays.stream(targetCell).sum() > Arrays.stream(tupleCell).sum()) {
        return true;
      } else if (Arrays.stream(targetCell).sum() == Arrays.stream(tupleCell).sum()) {
        for (int i = 0; i < targetCell.length; i++) { // compare individual coordinates
          if (targetCell[i] > tupleCell[i]) {
            return true;
          } else if (targetCell[i] == tupleCell[i]) {
            continue; // check the next dimension
          } else {
            return false;
          }
        }
      }

      return false;
    }

    private List<List<Double>> getTargetCellCorners(int[] cell, int cellLength ) {
      int dimensions = cell.length;
      int numCorners = (int) Math.pow(2, dimensions); // Total number of corners
      
      // Coordinates of all corners
      List<List<Double>> corners = new ArrayList<>();
      
      // Generate all possible combinations of offsets in each dimension
      for (int i = 0; i < numCorners; i++) {
          List<Double> currentCorner = new ArrayList<>();
          for (int j = 0; j < dimensions; j++) {
              double cornerCoordinate = (double) cell[j] * cellLength;
              if (((i >> j) & 1) == 1) {
                cornerCoordinate += cellLength;
              }
              currentCorner.add(cornerCoordinate);
          }
          corners.add(currentCorner);
      }

      return corners;
    }

    private Boolean isFarthestCornerInJoinRange(List<Double> tupleCoordinates, List<List<Double>> corners, Double joinRange) {
      double maxDistance = -1;
      for (List<Double> corner : corners) {
          EuclideanDistance distance = new EuclideanDistance();
          double dist = distance.calculate(tupleCoordinates, corner);
          if (dist > maxDistance) {
            maxDistance = dist;
          }
      }

      return maxDistance < joinRange;
    }

    private Boolean isNearestCornerInJoinRange(List<Double> tupleCoordinates, List<List<Double>> corners, Double joinRange) {
      double minDistance = Double.POSITIVE_INFINITY;
      for (List<Double> corner : corners) {
          EuclideanDistance distance = new EuclideanDistance();
          double dist = distance.calculate(tupleCoordinates, corner);
          if (dist < minDistance) {
            minDistance = dist;
          }
      }

      return minDistance < joinRange;
    }

    @Override
    public void execute(Tuple input) {
        List<Object> values;
        String tupleStreamId = input.getStringByField("streamId");
        // Stream tupleStream = this.schemaConfig.getStreamById(tupleStreamId);

        for (QueryGroup queryGroup : this.queryGroups) {
            // the tuple's stream is not relevant for this set of queries
            if (!queryGroup.isMemberStream(tupleStreamId)) {
                continue;
            }

            TupleWrapper tupleWrapper = new TupleWrapper(queryGroup.getAxisNamesSorted());
            List<Double> tupleCoordinates = tupleWrapper.getCoordinates(input, queryGroup.getDistance() instanceof CosineDistance);
            int dimensions = queryGroup.getAxisNamesSorted().size();
            int gridRange = (int) Math.ceil(queryGroup.maxJoinRadius / queryGroup.cellLength); // range in terms of number of cells
            int[] tupleCell = getTupleCell(tupleCoordinates, dimensions, queryGroup.cellLength); // eg. [-1, 0 1]

            List<int[]> enclosedCells = new ArrayList<>();
            List<int[]> intersectingCells = new ArrayList<>();

            for (int i = -gridRange + 1; i <= gridRange - 1; i++) {
              for (int j = -gridRange + 1; j <= gridRange - 1; j++) {
                int[] targetCell = new int[2]; // 2 dim
                targetCell[0] = tupleCell[0] + i;
                targetCell[1] = tupleCell[1] + j;
                
                if (!isValidTargetCell(targetCell, tupleCell)) {
                  continue;
                }

                List<List<Double>> corners = getTargetCellCorners(targetCell, queryGroup.cellLength);
                if (isFarthestCornerInJoinRange(tupleCoordinates, corners, queryGroup.maxJoinRadius)) {
                  enclosedCells.add(targetCell);
                } else if (isNearestCornerInJoinRange(tupleCoordinates, corners, queryGroup.maxJoinRadius)) {
                  intersectingCells.add(targetCell);
                }
              }
            }

            for (int[] cell : enclosedCells) {
              System.out.println(cell);
            }

            for (int[] cell : intersectingCells) {
              System.out.println(cell);
            }
          }
      }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        Stream stream = this.schemaConfig.getStreams().get(0);
        List<String> fields = new ArrayList<String>(stream.getFieldNames());
        fields.add("streamId");
        fields.add("clusterId"); // centroid of cube in this case
        fields.add("queryGroupName");
        declarer.declare(new Fields(fields));
    }
}
