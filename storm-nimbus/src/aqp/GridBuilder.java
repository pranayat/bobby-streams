package aqp;

import java.util.ArrayList;
import java.util.List;

public abstract class GridBuilder {
    public static List<Grid> build(List<JoinQuery> joinQueries) {
        List<Grid> grids = new ArrayList<>();
        for (JoinQuery joinQuery : joinQueries) {
            Grid grid = findGridByAxisNames(grids, joinQuery.getFieldsSorted());
            if (grid != null) {
                grid.registerJoinQuery(joinQuery);
            } else {
                grid = new Grid(joinQuery.getFieldsSorted());
                grid.registerJoinQuery(joinQuery);
                grids.add(grid);
            }
        }

        return grids;
    }

    private static Grid findGridByAxisNames(List<Grid> grids, List<String> axisNames) {
        for (Grid grid : grids) {
            if (axisNames.equals(grid.getAxisNamesSorted())) {
                return grid;
            }
        }

        return null;
    }
}
