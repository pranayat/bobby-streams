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
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class JoinerBoltNew extends BaseWindowedBolt {
    OutputCollector _collector;
    private SchemaConfig schemaConfig;
    List<JoinQuery> joinQueries;
    List<QueryGroup> queryGroups;

    public JoinerBoltNew() {
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

        try {
            for (Tuple tuple : inputWindow.getNew()) {
                QueryGroup queryGroup = getQueryGroupByName(tuple.getStringByField("queryGroupName"));
                String tupleClusterId = tuple.getStringByField("clusterId");
                String tupleStreamId = tuple.getStringByField("streamId");
                Boolean isReplica = tuple.getBooleanByField("isReplica");

                if (!isReplica) {
                    // count_min(C1_S1) ++
                    queryGroup.getPanakosCountSketch().add(tupleClusterId + "_" + tupleStreamId);

                    // count_min_sum(C1_S1) += S1.value for query1 SUM(S1.value)
                    // count_min_sum(C1_S1) += S1.velocity for query SUM(S1.velocity)
                    for (JoinQuery joinQuery : queryGroup.getJoinQueries()) {
                        String querySumStreamId = joinQuery.getSumStream();
                        String querySumField = joinQuery.getSumField();

                        if (tupleStreamId.equals(querySumStreamId)) {
                            joinQuery.getPanakosSumSketch().add(tupleClusterId + "_" + tupleStreamId, tuple.getDoubleByField(querySumField));
                        }

                        // join_count_C1_query1 = count_min(C1_S1) x count_min(C1_S2) x count_min(C1_S3), query1 = JOIN(S1 x S2 x S3)
                        int queryJoinCountForCluster = 1;
                        for (String streamId : joinQuery.getStreamIds()) {
                            queryJoinCountForCluster *= queryGroup.getPanakosCountSketch().query(tupleClusterId + "_" + streamId);
                        }

                        // query1 = SUM(S1.value)
                        // join_sum_C1_query1 = [ join_count_C1_query1 / count_min(C1_S1) ] x count_min_sum(C1_S1)
                        double queryJoinSumForCluster = (queryJoinCountForCluster / queryGroup.getPanakosCountSketch().query(tupleClusterId + "_" + querySumStreamId))
                                * joinQuery.getPanakosSumSketch().query(tupleClusterId + "_" + querySumStreamId);

                    }
                }

                    // for tuple's query group (the n-dim grid for this group of queries)
                        // count_min(C1_S1) ++
                        // count_min_sum(C1_S1) += T1.velocity TODO think about doing this at the sum column level as different columns could have very different ranges

                    // for each query (eg. query1=SUM(S1.velocity) S1xS2xS3) in the query group
                        // join_count_C1_query1 = count_min(C1_S1) x count_min(C1_S2) x count_min(C1_S3) // this is the count without replicas
                        // join_sum_C1_query1 = [ join_count_C1_query1 / count_min(C1_S1) ] x count_min_sum(C1_S1) // this is the sum without replicas

                        // find 'replica' join partners in index using join radius r of current query
                        // if join partners found in S2, S3 then for EACH joine partner eg. 'partner_tuple_S2'
                        //     join_count_C1_query1 += 1
                        //     join_sum_C1_query1 += partner_tuple_S1.velocity                    

            }
        } catch (Exception e) {
    
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        for (Query query : schemaConfig.getQueries()) {
            declarer.declareStream(query.getId() + "_resultStream", new Fields("queryId", "tupleId", "streamId"));
            declarer.declareStream(query.getId() + "_noResultStream", new Fields("queryId", "tupleId", "streamId"));
        }
    }
}
