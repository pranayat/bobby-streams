package aqp;

import java.util.ArrayList;
import java.util.List;

public abstract class QueryGroupBuilder {
    public static List<QueryGroup> build(List<JoinQuery> joinQueries) {
        List<QueryGroup> queryGroups = new ArrayList<>();
        for (JoinQuery joinQuery : joinQueries) {
            // create a new queryGroup for each axes+distance_type combination eg. [lat,long]+euclidean, [lat,long]+cosine, [lat,long, alt]+cosine
            QueryGroup queryGroup = findQueryGroupByAxisNames(queryGroups, joinQuery.getFieldsSorted(), joinQuery.getDistance());
            if (queryGroup != null) {
                queryGroup.registerJoinQuery(joinQuery);
            } else {
                queryGroup = new QueryGroup(joinQuery.getFieldsSorted(), joinQuery.getDistance(), joinQuery.getIDistance());
                queryGroup.registerJoinQuery(joinQuery);
                queryGroups.add(queryGroup);
            }
        }

        return queryGroups;
    }

    private static QueryGroup findQueryGroupByAxisNames(List<QueryGroup> queryGroups, List<String> axisNames, Distance distance) {
        for (QueryGroup queryGroup : queryGroups) {
            if (axisNames.equals(queryGroup.getAxisNamesSorted()) && distance.getClass().equals(queryGroup.getDistance().getClass())) {
                return queryGroup;
            }
        }

        return null;
    }
}
