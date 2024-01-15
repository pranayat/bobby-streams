package aqp;

import java.util.ArrayList;
import java.util.List;

public abstract class QueryGroupBuilder {
    public static List<QueryGroup> build(List<JoinQuery> joinQueries) {
        List<QueryGroup> queryGroups = new ArrayList<>();
        for (JoinQuery joinQuery : joinQueries) {
            QueryGroup queryGroup = findQueryGroupByAxisNames(queryGroups, joinQuery.getFieldsSorted());
            if (queryGroup != null) {
                queryGroup.registerJoinQuery(joinQuery);
            } else {
                queryGroup = new QueryGroup(joinQuery.getFieldsSorted());
                queryGroup.registerJoinQuery(joinQuery);
                queryGroups.add(queryGroup);
            }
        }

        return queryGroups;
    }

    private static QueryGroup findQueryGroupByAxisNames(List<QueryGroup> queryGroups, List<String> axisNames) {
        for (QueryGroup queryGroup : queryGroups) {
            if (axisNames.equals(queryGroup.getAxisNamesSorted())) {
                return queryGroup;
            }
        }

        return null;
    }
}
