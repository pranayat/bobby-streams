package aqp;

public abstract class JoinQueryBuilder {
  public static List<JoinQuery> buildJoinQueries(SchemaConfig schemaConfig) {
    List<JoinQuery> joinQueries = new ArrayList<>();
    for (Query query: schemaConfig.getQueries()) {
      for (Stage stage: query.getStages()) {
        if (query.getType().equals("join")) {
          joinQueries.add(new JoinQuery(stage.getMaxDistance(), stage.getBetween(), stage.getOn()));
        }
      }
    }

    return joinQueries;
  }
}
