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

public class KMeansClusterAssignerBolt extends BaseWindowedBolt {
    OutputCollector _collector;
    private SchemaConfig schemaConfig;
    List<JoinQuery> joinQueries;
    List<QueryGroup> queryGroups;

    public KMeansClusterAssignerBolt() {
        this.schemaConfig = SchemaConfigBuilder.build();
    }

    @Override
    public void prepare(
            Map stormConfig,
            TopologyContext topologyContext,
            OutputCollector collector) {
        _collector = collector;

        this.joinQueries = JoinQueryBuilder.build(this.schemaConfig);
        this.queryGroups = QueryGroupBuilder.build(this.joinQueries);
    }


    @Override
    public void execute(TupleWindow inputWindow) {

        for (QueryGroup queryGroup : this.queryGroups) {
            int k = this.schemaConfig.getClustering().getK(), iterations = this.schemaConfig.getClustering().getIterations();
            TupleWrapper tupleWrapper = new TupleWrapper(queryGroup.getAxisNamesSorted());
            KMeansClusterMaker clusterMaker = new KMeansClusterMaker(tupleWrapper, k, iterations, queryGroup.getDistance());
            List<Cluster> clusters = clusterMaker.fit(inputWindow.get());

            for (Cluster cluster : clusters) {
                for (Tuple tuple : cluster.getTuples()) {
                    String tupleStreamId = tuple.getSourceStreamId();
                    Stream tupleStream = this.schemaConfig.getStreamById(tupleStreamId);
                    List<Object> values;

                    values = new ArrayList<Object>();
                    for (Field field : tupleStream.getFields()) {
                        if (field.getType().equals("double")) {
                            values.add(tuple.getDoubleByField(field.getName()));
                        } else {
                            values.add(tuple.getStringByField(field.getName()));
                        }
                    }

                    values.add(cluster.getCentroid().toString()); // cluster Id eg. (25, 35)
                    values.add(queryGroup.getName()); // query group Id eg. (lat, long)

                    // stream_1, (25,35) (lat,long) ...
                    _collector.emit(tupleStreamId, new Values(values.toArray()));
                }
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        for (Stream stream : this.schemaConfig.getStreams()) {
            List<String> fields = new ArrayList<String>(stream.getFieldNames());
            fields.add("clusterId"); // centroid of cluster in this case
            fields.add("queryGroupName");
            declarer.declareStream(stream.getId(), new Fields(fields));
        }
    }
}
