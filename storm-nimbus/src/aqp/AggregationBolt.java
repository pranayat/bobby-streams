package aqp;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.Optional;

public class AggregationBolt extends BaseWindowedBolt {
    OutputCollector _collector;
    private SchemaConfig schemaConfig;
    List<JoinQuery> joinQueries;
    List<QueryGroup> queryGroups;

    public AggregationBolt() {
        this.schemaConfig = SchemaConfigBuilder.build();
    }

    @Override
    public void prepare(
            Map map,
            TopologyContext topologyContext,
            OutputCollector collector) {
        _collector = collector;

        this.joinQueries = JoinQueryBuilder.build(this.schemaConfig);
    }

    private JoinQuery getJoinQueryById(String queryId) {
      // TODO make this a hashmap
      return this.joinQueries.stream()
              .filter(g -> g.getId().equals(queryId))
              .findFirst()
              .get();
    }

    @Override
    public void execute(TupleWindow inputWindow) {

        try {
            for (Tuple tuple : inputWindow.getNew()) {
                String clusterId = tuple.getStringByField("clusterId");
                Integer tupleApproxJoinCount = tuple.getIntegerByField("tupleApproxJoinCount");
                Double tupleApproxJoinSum = tuple.getDoubleByField("tupleApproxJoinSum");
                String joinQueryId = tuple.getStringByField("queryId");
                JoinQuery joinQuery = getJoinQueryById(joinQueryId);

                joinQuery.getClusterJoinCountMap().put(clusterId, Optional.ofNullable(joinQuery.getClusterJoinCountMap()).map(map -> map.get(clusterId)).orElse(0) + tupleApproxJoinCount);
                joinQuery.getClusterJoinSumMap().put(clusterId, Optional.ofNullable(joinQuery.getClusterJoinSumMap()).map(map -> map.get(clusterId)).orElse(0.0) + tupleApproxJoinSum);

                // aggregate counts and sums across clusters
                Integer joinCount = joinQuery.aggregateJoinCounts();
                Double joinSum = joinQuery.aggregateJoinSums();
                Double joinAvg = 0.0;

                if (joinCount != 0) {
                  joinAvg = joinSum / joinCount;
                }

                List<Object> values = new ArrayList<Object>();
                values.add(joinQuery.getId());
                values.add(joinCount);
                values.add(joinSum);
                values.add(joinAvg);

                _collector.emit(joinQuery.getId() + "_aggregateStream", tuple, values);
              }

              for (Tuple expiredTuple : inputWindow.getExpired()) {
                // deduct expired counts and sums
                String clusterId = expiredTuple.getStringByField("clusterId");
                Integer tupleApproxJoinCount = expiredTuple.getIntegerByField("tupleApproxJoinCount");
                Double tupleApproxJoinSum = expiredTuple.getDoubleByField("tupleApproxJoinSum");
                String joinQueryId = expiredTuple.getStringByField("queryId");
                JoinQuery joinQuery = getJoinQueryById(joinQueryId);

                joinQuery.getClusterJoinCountMap().put(clusterId, Optional.ofNullable(joinQuery.getClusterJoinCountMap()).map(map -> map.get(clusterId)).orElse(0) - tupleApproxJoinCount);
                joinQuery.getClusterJoinSumMap().put(clusterId, Optional.ofNullable(joinQuery.getClusterJoinSumMap()).map(map -> map.get(clusterId)).orElse(0.0) - tupleApproxJoinSum);
              }
        } catch (Exception e) {
              e.printStackTrace(System.out);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      for (Query query : schemaConfig.getQueries()) {
        declarer.declareStream(query.getId() + "_aggregateStream", new Fields("queryId", "joinCount", "joinSum", "joinAvg"));
      }
    }
}
