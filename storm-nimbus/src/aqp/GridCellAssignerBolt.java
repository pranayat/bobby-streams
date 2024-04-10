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
import java.util.stream.Collectors;
import java.util.Arrays;

public class GridCellAssignerBolt extends BaseRichBolt {
    OutputCollector _collector;
    private SchemaConfig schemaConfig;
    List<JoinQuery> joinQueries;
    List<QueryGroup> queryGroups;
    Integer tupleCount = 0;

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

    private double findMinimumNthCoordinate(List<List<Double>> points, Integer n) {
      double min = Double.POSITIVE_INFINITY;

      for (List<Double> point : points) {
          double c = point.get(n); // Extract nth-coordinate from the point
          min = Math.min(min, c); // Update minX if the current x is smaller
      }

      return min;
    }

    private double findMaximumNthCoordinate(List<List<Double>> points, Integer n) {
      double min = Double.NEGATIVE_INFINITY;

      for (List<Double> point : points) {
          double c = point.get(n); // Extract nth-coordinate from the point
          min = Math.max(min, c); // Update minX if the current x is smaller
      }

      return min;
    }

    // Find the intersection coordinate in the ith dimension, given coordinates in all other dimensions
    private Double findXi(List<Double> centerCoordinates, double radius, List<Double> intersectionPoint, int index) {
      Double sumSquares = 0.0;

      // Calculate the sum of squares of differences for each coordinate (except xi)
      for (int i = 0; i < intersectionPoint.size(); i++) {
          if (i != index) { // Exclude xi
              Double diff = intersectionPoint.get(i) - centerCoordinates.get(i);
              sumSquares += diff * diff;
          }
      }

      // Calculate the value of xi using the formula
      Double xi = Math.sqrt(radius * radius - sumSquares) + centerCoordinates.get(index);
      
      return xi;
    }

    private List<Double> getMinsByDimension(List<List<Double>> corners) {
      List<Double> minElements = new ArrayList<>();
      int size = corners.get(0).size(); // Get the size of the first inner list
      for (int i = 0; i < size; i++) {
          final int index = i; // Using effectively final variable in lambda
          Double min = corners.stream()
                              .map(innerList -> innerList.get(index))
                              .min(Double::compareTo)
                              .orElse(Double.NaN); // Handle the case where the list is empty
          minElements.add(min);
      }
      return minElements;
    }

    private List<Double> getMaxesByDimension(List<List<Double>> corners) {
      List<Double> maxElements = new ArrayList<>();
      int size = corners.get(0).size(); // Get the size of the first inner list
      for (int i = 0; i < size; i++) {
          final int index = i; // Using effectively final variable in lambda
          Double max = corners.stream()
                              .map(innerList -> innerList.get(index))
                              .max(Double::compareTo)
                              .orElse(Double.NaN); // Handle the case where the list is empty
          maxElements.add(max);
      }
      return maxElements;
    }

    private List<List<String>> generateCombinations(int n) {
      List<List<String>> combinations = new ArrayList<>();
      // Iterate from 0 to 2^n - 1
      for (int i = 0; i < (1 << n); i++) {
          List<String> combination = new ArrayList<>();
          // Convert the current number to binary and generate the combination
          for (int j = 0; j < n; j++) {
              combination.add(j, ((i >> j) & 1) == 0 ? "min" : "max");
          }
          combinations.add(combination);
      }
      return combinations;
  }

    private List<List<Double>> findSphereCubeIntersectionPoints(List<Double> minsByDimension, List<Double> maxesByDimension, List<Double> centerCoordinates, Double radius, Integer dimensionality) {
      List<List<String>> minMaxCombinations = generateCombinations(dimensionality); // find edge intersection coordinate by substituting min/max for other coordinates in sphere equation

      List<List<Double>> intersectionPoints = new ArrayList<>();

      // find x given (_, y_min, z_min), (_, y_min, z_max), (_, y_max, z_min), (_, y_max, z_max)
      // find y given (x_min, _, z_min), (x_min, _, z_max), (x_max, _, z_min), (x_max, _, z_max)
      for (int i = 0; i < dimensionality; i++) {

        
        for (List<String> minMaxCombination : minMaxCombinations) { // (_, min, min)

          List<Double> intersectionPoint = new ArrayList<>(dimensionality);
          int j = 0;
          for (String minMax : minMaxCombination) { // min
            
            if (j != i) { // if j == i leave it null and populat the others with min/max
              if (minMax.equals("min")) {
                intersectionPoint.add(j, minsByDimension.get(j));
              } else {
                intersectionPoint.add(j, maxesByDimension.get(j));
              }
            } else {
              intersectionPoint.add(j, null); // this is _ where we will later add the computed coordinate
            }
            j++;
          }

          Double coordinate = findXi(centerCoordinates, radius, intersectionPoint, i);

          // this coordinate should be in the valid range for this point to be intersecting the grid cell's edge
          if (minsByDimension.get(i) <= coordinate && coordinate <= maxesByDimension.get(i)) {
            intersectionPoint.set(i, coordinate);
            intersectionPoints.add(intersectionPoint);
          }
        }
      }

      return intersectionPoints;
    }
    
    private List<Integer> findFreeDimensions(List<List<Double>> intersectionPoints, List<Double> minsByDimension, List<Double> maxesByDimension, Integer dimensionality) {
      List<Integer> freeDimensions = new ArrayList<>();

      for (int i = 0; i < dimensionality; i++) {
        for (List<Double> intersectionPoint : intersectionPoints) {
          if (intersectionPoint.get(i) != minsByDimension.get(i) && intersectionPoint.get(i) != maxesByDimension.get(i)) {
            freeDimensions.add(i);
            break; // check next dimension now, no need to go through all points
          }
        }
      }

      return freeDimensions;
    }

    private List<Integer> findFixedDimensions(List<Integer> freeDimensions, Integer dimensionality) {
      List<Integer> fixedDimensions = new ArrayList<>();
      
      for (int i = 0; i < dimensionality; i++) {
        if (!freeDimensions.contains(i)) {
          fixedDimensions.add(i);
        }
      }

      return fixedDimensions;
    }

    private Double getIntersectionVolumeRatio(List<Double> tupleCoordinates, List<List<Double>> corners, Double joinRange, Integer cellLength) {
      Integer dimensionality = tupleCoordinates.size();

      // get max and min values of the grid coordinates in each dimension
      List<Double> maxesByDimension = getMaxesByDimension(corners); // [x_max, y_max, z_max]
      List<Double> minsByDimension = getMinsByDimension(corners); // [x_min, y_min, z_min]

      List<List<Double>> intersectionPoints = findSphereCubeIntersectionPoints(minsByDimension, maxesByDimension, tupleCoordinates, joinRange, dimensionality);
      List<Integer> freeDimensions = findFreeDimensions(intersectionPoints, minsByDimension, maxesByDimension, dimensionality); // eg. [1, 3, 4]
      List<Integer> fixedDimensions = findFixedDimensions(freeDimensions, dimensionality); // eg. [0] if only x is fixed, [] if none are fixed (corner approach)

      Double volume = 1.0;

      for (Integer freeDimension : freeDimensions) {
        List<Double> intersectionCoordinatesForFreeDimension = intersectionPoints.stream()
          .map(innerList -> innerList.get(freeDimension))
          .collect(Collectors.toList());

        // eg. if freeDimension = 1 ie. y, so check if y_sphere > y_max
        if (tupleCoordinates.get(freeDimension) > maxesByDimension.get(freeDimension)) {
          volume *= Math.abs(maxesByDimension.get(freeDimension) - intersectionCoordinatesForFreeDimension.stream().min(Double::compareTo).orElse(Double.NaN));
        } else {
          volume *= Math.abs(intersectionCoordinatesForFreeDimension.stream().max(Double::compareTo).orElse(Double.NaN) - minsByDimension.get(freeDimension));
        }
      }

      for (Integer fixedDimension : fixedDimensions) {
        volume *= Math.abs(maxesByDimension.get(fixedDimension) - minsByDimension.get(fixedDimension));
      }

      return Math.abs(volume / Math.pow(cellLength, dimensionality));
    }

    @Override
    public void execute(Tuple input) {
        
        if (tupleCount == 10) {
          return;
        }

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
                      volumeRatio = getIntersectionVolumeRatio(tupleCoordinates, corners, joinQuery.radius, queryGroup.cellLength);
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
          
          tupleCount++;
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
