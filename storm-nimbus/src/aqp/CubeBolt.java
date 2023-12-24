package aqp;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.windowing.TupleWindow;

import java.util.List;
import java.util.Map;

public class CubeBolt extends BaseWindowedBolt {
    OutputCollector _collector;
    private JoinQueryCache joinQueryCache;
    private SchemaConfig schemaConfig;
    private List<List<String>> joinIndices;

    public CubeBolt() {
        this.schemaConfig = SchemaConfigBuilder.build();
        this.joinIndices = this.schemaConfig.getJoinIndices();
    }

    @Override
    public void prepare(
            Map map,
            TopologyContext topologyContext,
            OutputCollector collector) {
        _collector = collector;

        this.joinQueryCache = JoinQueryCache.getInstance();
    }

    @Override
    public void execute(TupleWindow inputWindow) {

        List<String> joinIndex = this.joinIndices.get(0);
        TupleWrapper tupleWrapper = new TupleWrapper(joinIndex);
        ClusterMaker clusterMaker = new ClusterMaker(tupleWrapper);
        Map<List<Double>, List<Tuple>> cluster = clusterMaker.fit(inputWindow.get(), 3, 100);

        System.out.println(cluster);
//        for (Tuple tuple : inputWindow.get()) {
//
//            // for each stream
//            // if the tuple belongs to this stream's spatial index
//            for (Map.Entry<String, List<String>> stream : this.schemaConfig.getStreams().entrySet()) {
//
//            }
//        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("cubeId", "lat", "long", "alt", "text"));
    }
}
