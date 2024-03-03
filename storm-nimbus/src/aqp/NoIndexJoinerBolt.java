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
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class NoIndexJoinerBolt extends BaseWindowedBolt {
    OutputCollector _collector;
    private SchemaConfig schemaConfig;
    List<JoinQuery> joinQueries;
    List<QueryGroup> queryGroups;
    List<Tuple> currentWindow;

    public NoIndexJoinerBolt() {
        this.schemaConfig = SchemaConfigBuilder.build();
    }

    @Override
    public void prepare(
            Map map,
            TopologyContext topologyContext,
            OutputCollector collector) {
        _collector = collector;

        this.joinQueries = JoinQueryBuilder.build(this.schemaConfig);
        this.queryGroups = QueryGroupBuilder.build(this.joinQueries);
        this.currentWindow = new ArrayList<>();
    }

    private QueryGroup getQueryGroupByName(String queryGroupName) {
        // TODO make this a hashmap
        return this.queryGroups.stream()
                .filter(g -> g.getName().equals(queryGroupName))
                .findFirst()
                .get();
    }

    @Override
    public void execute(TupleWindow inputWindow) {

        // getNew() will return the one new tuple that slides in
        for (Tuple tuple : inputWindow.getNew()) {
            QueryGroup queryGroup = getQueryGroupByName(tuple.getStringByField("queryGroupName"));

            for (JoinQuery joinQuery : queryGroup.getJoinQueries()) {
                List<Tuple> joinResults = joinQuery.executeNoIndex(tuple, queryGroup, currentWindow);
                List<String> tupleIds = new ArrayList<>();
                for (Tuple joinResult : joinResults) {
                    tupleIds.add(joinResult.getStringByField("tupleId"));
                }

                Collections.sort(tupleIds);
                String joinId = String.join("+", tupleIds);

                for (Tuple joinResult : joinResults) {
                    _collector.emit("resultStream", tuple, new Values(joinId, joinResult.getStringByField("tupleId"), joinResult.getStringByField("streamId")));
                }

                if (joinResults.size() == 0) {
                    _collector.emit("noResultStream", tuple, new Values(tuple.getStringByField("tupleId"), tuple.getStringByField("streamId")));
                }
            }

            // a simple list instead of a B+tree
            currentWindow.add(tuple);
        }

        for (int i = 0; i < inputWindow.getExpired().size(); i++) {
            currentWindow.remove(currentWindow.size() - 1); // remove last element which will also be the oldest element
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("resultStream", new Fields("joinId", "tupleId", "streamId"));
        declarer.declareStream("noResultStream", new Fields("tupleId", "streamId"));
    }
}
