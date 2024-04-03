package aqp;

import java.util.ArrayList;
import java.util.List;

public abstract class JoinQueryBuilder {
    public static List<JoinQuery> build(SchemaConfig schemaConfig) {
        List<JoinQuery> joinQueries = new ArrayList<>();
        for (Query query : schemaConfig.getQueries()) {
            JoinQuery joinQuery = null;
            for (Stage stage : query.getStages()) {
                if (stage.getType().equals("join")) {
                    Distance distance;
                    IDistance iDistance;
                    if (stage.getDistanceType().equals("cosine")) {
                        distance = new CosineDistance();
                        iDistance = new CosineIDistance();
                    } else {
                        distance = new EuclideanDistance();
                        iDistance = new EuclideanIDistance();
                    }

                    // join stage always comes befor sum stage so joinQuery will have been created here when we go to the else if
                    joinQuery = new JoinQuery(query.getId(), stage.getRadius(), stage.getBetween(), stage.getOn(), distance, iDistance);
                    joinQueries.add(joinQuery);
                } else if (stage.getType().equals("avg")) {
                    joinQuery.setAggregateStream(stage.getAggregateStream());
                    joinQuery.setAggregatableFields(stage.getAggregatableFields());
                }
            }
        }

        return joinQueries;
    }
}
