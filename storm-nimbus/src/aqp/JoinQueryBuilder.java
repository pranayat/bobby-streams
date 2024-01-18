package aqp;

import java.util.ArrayList;
import java.util.List;

public abstract class JoinQueryBuilder {
    public static List<JoinQuery> build(SchemaConfig schemaConfig) {
        List<JoinQuery> joinQueries = new ArrayList<>();
        for (Query query : schemaConfig.getQueries()) {
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
                    joinQueries.add(new JoinQuery(stage.getRadius(), stage.getBetween(), stage.getOn(), distance, iDistance));
                }
            }
        }

        return joinQueries;
    }
}
