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
    private BPlusTree bPlusTree;

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

        // TODO just dealing with lat,long index for now
        List<String> joinIndex = this.joinIndices.get(0);
        TupleWrapper tupleWrapper = new TupleWrapper(joinIndex);
        ClusterMaker clusterMaker = new ClusterMaker(tupleWrapper);
        List<Cluster> clusters = clusterMaker.fit(inputWindow.get(), 3, 100);
        this.bPlusTree = new BPlusTree(512);

        int i = 1;
        int c = 100000;
        Distance distance = new EuclideanDistance();
        for (Cluster cluster : clusters) {
            for (Tuple tuple : cluster.getTuples()) {
                this.bPlusTree.insert(i * c + distance.calculate(cluster.getCentroid(), tupleWrapper.getCoordinates(tuple)), tuple);
            }
            i += 1;
        }
        System.out.println(this.bPlusTree);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("cubeId", "lat", "long", "alt", "text"));
    }
}
