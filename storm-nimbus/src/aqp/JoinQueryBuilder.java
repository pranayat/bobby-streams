package aqp;

import java.util.ArrayList;
import java.util.List;

public abstract class JoinQueryBuilder {
    public static List<JoinQuery> build(SchemaConfig schemaConfig) {
        List<JoinQuery> joinQueries = new ArrayList<>();
        for (Query query : schemaConfig.getQueries()) {
            for (Stage stage : query.getStages()) {
                if (stage.getType().equals("join")) {
                    joinQueries.add(new JoinQuery(stage.getMaxDistance(), stage.getBetween(), stage.getOn()));
                }
            }
        }

        return joinQueries;
    }
}
