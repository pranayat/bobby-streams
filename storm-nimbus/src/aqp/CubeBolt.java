package aqp;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.windowing.TupleWindow;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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
        int joinRadius = 10;

        int c = 100000;
        Distance distance = new EuclideanDistance();
        for (Tuple tuple : inputWindow.get()) {
            List<Tuple> joinCandidates = new ArrayList<>();
            for (Cluster cluster : clusters) {
                double queryTupleToCentroidDistance = distance.calculate(cluster.getCentroid(), tupleWrapper.getCoordinates(tuple));
                /* if tuple is inside cluster sphere
                then search between i * c + dist(O_i, q) - join_radius
                and
                min(i * c + dist_max_i, i * c + dist(O_i,q) + join_radius)
                 */
                if (queryTupleToCentroidDistance < cluster.getRadius()) {
                    joinCandidates.addAll(bPlusTree.search(cluster.getI() * c + queryTupleToCentroidDistance - joinRadius,
                            Math.min(cluster.getI() * c + cluster.getRadius(), cluster.getI() * c + queryTupleToCentroidDistance + joinRadius)));
                }
                /* tuple is outside the cluster sphere but its query sphere intersects the cluster sphere
                then search between i * c + dist(O_i, q) - join_radius
                and
                i * c + dist(O_i, dist_max_i)
                 */
                else if (queryTupleToCentroidDistance < cluster.getRadius() + joinRadius) {
                    joinCandidates.addAll(this.bPlusTree.search(cluster.getI() * c + queryTupleToCentroidDistance - joinRadius,
                            cluster.getI() * c + cluster.getRadius()));
                }

                // don't join with the same stream
                List<Tuple> filteredList = joinCandidates.stream()
                        .filter(joinCandidate -> !tuple.getSourceStreamId().equals(joinCandidate.getSourceStreamId()))
                        .collect(Collectors.toList());

                this.bPlusTree.insert(cluster.getI() * c + distance.calculate(cluster.getCentroid(), tupleWrapper.getCoordinates(tuple)), tuple);
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("cubeId", "lat", "long", "alt", "text"));
    }
}
