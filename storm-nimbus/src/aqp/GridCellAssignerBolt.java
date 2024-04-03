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

    private Double getIntersectionVolumeRatio(List<Double> tupleCoordinates, List<List<Double>> corners, Double joinRange) {
      return 0.5;
    }

    @Override
    public void execute(Tuple input) {
        String tupleStreamId = input.getStringByField("streamId");

        List<int[]> replicationCells = new ArrayList<>();
        for (QueryGroup queryGroup : this.queryGroups) {
            // the tuple's stream is not relevant for this set of queries
            if (!queryGroup.isMemberStream(tupleStreamId)) {
                continue;
            }

            TupleWrapper tupleWrapper = new TupleWrapper(queryGroup.getAxisNamesSorted());
            List<Double> tupleCoordinates = tupleWrapper.getCoordinates(input, queryGroup.getDistance() instanceof CosineDistance);
            int dimensions = queryGroup.getAxisNamesSorted().size();
            // maxJoinRadius as we want to send it as far as needed for the query with the largest join radius
            int gridRange = (int) Math.ceil(queryGroup.maxJoinRadius / queryGroup.cellLength); // range in terms of number of cells
            int[] tupleCell = getTupleCell(tupleCoordinates, dimensions, queryGroup.cellLength); // eg. [-1, 0 1]

            int[] targetCell = new int[dimensions]; // Create an array to store the coordinates of the target cell
            int[] indices = new int[dimensions]; // Create an array to store the current index for each dimension
            Arrays.fill(indices, -gridRange + 1); // Initialize all indices to the starting value (-gridRange + 1)

            while (indices[0] <= gridRange - 1) { // Continue looping until the first index reaches the maximum value (gridRange - 1)
                for (int i = 0; i < dimensions; i++) { // Iterate over each dimension
                    targetCell[i] = tupleCell[i] + indices[i]; // Calculate the coordinate of the target cell for the current dimension
                }
            
                Boolean isReplica = true;
                if (Arrays.equals(tupleCell, targetCell)) { // is tuple's home cell
                  isReplica = false;
                }

                // emit only to the home cell of the tuple (isReplica=false) OR a valid replication target
                if (!isReplica || isValidTargetCell(targetCell, tupleCell)) {
                  // add this target cell once per query group and not multiple times for each individual query
                  replicationCells.add(targetCell);

                  List<List<Double>> corners = getTargetCellCorners(targetCell, queryGroup.cellLength);
                  String enclosedBy = "enclosedBy:"; // eg. enclosedBy:,query1,query2 ie. wrt to query1 and query2 radii, this cell is completely enclosed
                  String intersectedBy = "intersectedBy:";
                  Double volumeRatio = 1.0;
                  for (JoinQuery joinQuery : queryGroup.joinQueries) {
                    if (isFarthestCornerInJoinRange(tupleCoordinates, corners, joinQuery.radius)) {
                      enclosedBy += "," + joinQuery.getId();
                    } else if (isNearestCornerInJoinRange(tupleCoordinates, corners, joinQuery.radius)) {
                      volumeRatio = getIntersectionVolumeRatio(tupleCoordinates, corners, joinQuery.radius);
                      intersectedBy += "," + joinQuery.getId() + ":ratio=" + volumeRatio;
                    }
                  }

                  // just insert tuple's existing fields
                  Stream tupleStream = this.schemaConfig.getStreamById(tupleStreamId);
                  List<Object> values = new ArrayList<Object>();
                  for (Field field : tupleStream.getFields()) {
                      if (field.getType().equals("double")) {
                          values.add(input.getDoubleByField(field.getName()));
                      } else {
                          values.add(input.getStringByField(field.getName()));
                      }
                  }

                  // isReplica = true
                  values.add(isReplica);

                  // add query's this cell is enclosed, intersected by
                  values.add(enclosedBy);
                  values.add(intersectedBy);

                  values.add(tupleStreamId); // stream_1

                  // add cell centroid
                  List<Double> centroid = new ArrayList<>();
                  // for cube [2,3] cell length 10 with centroid [2*10 + 10/2, 3*10 + 10/2 ][25, 35]
                  for (int axisOffset : targetCell) {
                      centroid.add((double) (axisOffset * queryGroup.getCellLength() + queryGroup.getCellLength() / 2));
                  }

                  values.add(centroid.toString()); // cluster Id eg. (25, 35)
                  values.add(queryGroup.getName()); // query group Id eg. (lat, long)

                  // enclosedBy:query1,query2 intersectedBy:query3 stream_1, (25,35) (lat,long) ...
                  _collector.emit(input, new Values(values.toArray()));

                }

                // Update indices for next iteration
                indices[dimensions - 1]++; // Increment the last index
                for (int i = dimensions - 1; i > 0; i--) { // Iterate backwards over the dimensions
                    if (indices[i] > gridRange - 1) { // Check if the current index exceeds the maximum value
                        indices[i] = -gridRange + 1; // Reset the current index to the starting value
                        indices[i - 1]++; // Increment the index of the previous dimension
                    }
                }
            }
          }

          _collector.ack(input);
      }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        Stream stream = this.schemaConfig.getStreams().get(0);
        List<String> fields = new ArrayList<String>(stream.getFieldNames());
        fields.add("isReplica");
        fields.add("enclosedBy"); // queries this cell is enclosed by
        fields.add("intersectedBy"); // queries this cell is intersected by
        fields.add("streamId");
        fields.add("clusterId"); // centroid of cube in this case
        fields.add("queryGroupName");
        declarer.declare(new Fields(fields));
    }
}
