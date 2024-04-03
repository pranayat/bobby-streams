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

public class AggregationBoltV2 extends BaseRichBolt {
    OutputCollector _collector;
    private SchemaConfig schemaConfig;
    List<JoinQuery> joinQueries;
    List<QueryGroup> queryGroups;

    public AggregationBoltV2() {
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

    @Override
    public void execute(Tuple tuple) {

      try {
            String joinQueryId = extractJoinQueryId(tuple.getSourceStreamId());
            JoinQuery joinQuery = getJoinQueryById(joinQueryId);
            Boolean isExpired = tuple.getBooleanByField("isExpired");

            // all tuples with the same value for the groupByField will end up in this joiner
            if (isExpired) {
              joinQuery.removeFromCountSketchV2(tuple);
              joinQuery.removeFromSumSketchV2(tuple);
            } else {
              joinQuery.addToCountSketchV2(tuple);
              joinQuery.addToSumSketchV2(tuple);

              double count = joinQuery.getPanakosCountSketch().query("v2");
              double sum = joinQuery.getPanakosSumSketch().query("v2");
              double avg = 0.0;
  
              if (count != 0) {
                avg = sum / count;
              }
  
              List<Object> values = new ArrayList<Object>();
              values.add(joinQuery.getId());
              values.add(count);
              values.add(sum);
              values.add(avg);
  
              _collector.emit(joinQuery.getId() + "-aggregateResultStream", tuple, values);
            }
        } catch (Exception e) {
              e.printStackTrace(System.out);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      for (Query query : schemaConfig.getQueries()) {
        declarer.declareStream(query.getId() + "-aggregateResultStream", new Fields("queryId", "count", "sum", "avg"));
      }
    }
}
