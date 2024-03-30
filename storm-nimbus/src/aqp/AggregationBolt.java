package aqp;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.Optional;

public class AggregationBolt extends BaseRichBolt {
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

    private String extractJoinQueryId(String sourceStream) {
      return sourceStream.split("-")[0];
    }

    private String extractGroupByField(String sourceStream) {
      return sourceStream.split(":")[1];
    }    

    @Override
    public void execute(Tuple tuple) {

      try {
            String joinQueryId = extractJoinQueryId(tuple.getSourceStreamId());
            String groupByField = extractGroupByField(tuple.getSourceStreamId());
            JoinQuery joinQuery = getJoinQueryById(joinQueryId);

            // we know all tuples with the same value for the groupByField will end up in this joiner
            joinQuery.addToCountSketch(tuple, groupByField);
            joinQuery.addToSumSketch(tuple, groupByField);
            
            double count = joinQuery.getPanakosCountSketch().query("stream_1.time=2100.0");
            double sum = joinQuery.getPanakosSumSketch().query("stream_1.time=2100.0");
            double avg = sum / count;
            // Double joinAvg = 0.0;

            // if (joinCount != 0) {
            //   joinAvg = joinSum / joinCount;
            // }

            List<Object> values = new ArrayList<Object>();
            values.add(joinQuery.getId());
            values.add("stream_1.time=2100.0");
            values.add(count);
            values.add(sum);
            values.add(avg);

            _collector.emit(joinQuery.getId() + "_aggregateResultStream", tuple, values);

            // for (Tuple expiredTuple : inputWindow.getExpired()) {
            //   // deduct expired counts and sums
            //   String clusterId = expiredTuple.getStringByField("clusterId");
            //   Integer tupleApproxJoinCount = expiredTuple.getIntegerByField("tupleApproxJoinCount");
            //   Double tupleApproxJoinSum = expiredTuple.getDoubleByField("tupleApproxJoinSum");
            //   String joinQueryId = expiredTuple.getStringByField("queryId");
            //   JoinQuery joinQuery = getJoinQueryById(joinQueryId);

            //   joinQuery.getClusterJoinCountMap().put(clusterId, Optional.ofNullable(joinQuery.getClusterJoinCountMap()).map(map -> map.get(clusterId)).orElse(0) - tupleApproxJoinCount);
            //   joinQuery.getClusterJoinSumMap().put(clusterId, Optional.ofNullable(joinQuery.getClusterJoinSumMap()).map(map -> map.get(clusterId)).orElse(0.0) - tupleApproxJoinSum);
            // }
        } catch (Exception e) {
              e.printStackTrace(System.out);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      for (Query query : schemaConfig.getQueries()) {
        declarer.declareStream(query.getId() + "_aggregateResultStream", new Fields("queryId", "havingClause", "count", "sum", "avg"));
      }
    }
}
