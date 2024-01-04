package aqp;

public abstract class GridBuilder {
  public static List<Grid> buildGrids(List<JoinQuery> joinQueries) {
    List<Grid> grids = new ArrayList<>();
    for (JoinQuery joinQuery : joinQueries) {
      Grid grid = findGridByAxisNames(grids, joinQuery.getFields());
      if (grid) {
        grid.registerQuery(joinQuery);
      } else {
        grid = new Grid(joinQuery.getFields());
        grid.registerJoinQuery(joinQuery);
        grids.add(grid);
      }
    }
  }

  private Grid findGridByAxisNames(List<Grid> grids, List<String> axisNames) {
    for (Grid grid : grids) {
      if (axisNames.equals(grid.getAxisNames())) {
        return grid;
      }
    }
  }
}
