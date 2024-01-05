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

public class CubeBolt extends BaseWindowedBolt {
    OutputCollector _collector;
    private SchemaConfig schemaConfig;
    List<JoinQuery> joinQueries;
    List<Grid> grids;

    public CubeBolt() {
        this.schemaConfig = SchemaConfigBuilder.build();
    }

    @Override
    public void prepare(
            Map map,
            TopologyContext topologyContext,
            OutputCollector collector) {
        _collector = collector;

        this.joinQueries = JoinQueryBuilder.build(this.schemaConfig);
        this.grids = GridBuilder.build(this.joinQueries);
    }

    @Override
    public void execute(TupleWindow inputWindow) {

        for (Grid grid : this.grids) {
            TupleWrapper tupleWrapper = new TupleWrapper(grid.getAxisNamesSorted());
            ClusterMaker clusterMaker = new GridClusterMaker(tupleWrapper, grid.getCellLength());
            //        ClusterMaker clusterMaker = new KMeansClusterMaker(tupleWrapper, 3, 100);
            List<Cluster> clusters = clusterMaker.fit(inputWindow.get());
            grid.setClusters(clusters);
            grid.setBPlusTree(new BPlusTree(512));
        }

        int c = 100000;
        Distance distance = new EuclideanDistance();
        /* For each tuple in the window
            Find join partners in B+trees of all relevant grids, for each join query registered in that grid
            B+tree is indexed by the iDistance of the tuple to the closest cluster for that grid
            Join partners are looked up using iDistance of the tuple to all clusters enclosing or intersecting the tuple's query sphere
         */
        for (Tuple tuple : inputWindow.get()) {
            Grid grid = this.grids.stream()
                    .filter(g -> g.getName().equals(tuple.getStringByField("gridName")))
                    .findFirst()
                    .get();
            Cluster closestCluster = null;
            double closestClusterDistance = -1;
            TupleWrapper tupleWrapper = new TupleWrapper(grid.getAxisNamesSorted());
            BPlusTree bPlusTree = grid.getBPlusTree();
            for (Cluster cluster : grid.getClusters()) {
                double queryTupleToCentroidDistance = distance.calculate(cluster.getCentroid(), tupleWrapper.getCoordinates(tuple));

                for (JoinQuery joinQuery : grid.getJoinQueries()) {
                    List<Tuple> joinCandidates = new ArrayList<>();
                    /* if tuple is inside cluster sphere
                    then search between i * c + dist(O_i, q) - join_radius
                    and
                    min(i * c + dist_max_i, i * c + dist(O_i,q) + join_radius)
                    */
                    if (queryTupleToCentroidDistance < cluster.getRadius()) {
                        joinCandidates.addAll(bPlusTree.search(cluster.getI() * c + queryTupleToCentroidDistance - joinQuery.getRadius(),
                                Math.min(cluster.getI() * c + cluster.getRadius(), cluster.getI() * c + queryTupleToCentroidDistance + joinQuery.getRadius())));
                    }
                    /* tuple is outside the cluster sphere but its query sphere intersects the cluster sphere
                    then search between i * c + dist(O_i, q) - join_radius
                    and
                    i * c + dist(O_i, dist_max_i)
                    */
                    else if (queryTupleToCentroidDistance < cluster.getRadius() + joinQuery.getRadius()) {
                        joinCandidates.addAll(bPlusTree.search(cluster.getI() * c + queryTupleToCentroidDistance - joinQuery.getRadius(),
                                cluster.getI() * c + cluster.getRadius()));
                    }

                    for (Tuple joinCandidate : joinCandidates) {
                        if (tuple.getSourceStreamId().equals(joinCandidate.getSourceStreamId())) {
                            continue;
                        }
                        List<Tuple> joinResult = new ArrayList<Tuple>(); // holds the join pair
                        joinResult.add(tuple);
                        joinResult.add(joinCandidate);
                        joinQuery.addResult(joinResult); // add join pair to result list
                    }
                }

                if (closestClusterDistance == -1 || queryTupleToCentroidDistance < closestClusterDistance) {
                    closestClusterDistance = queryTupleToCentroidDistance;
                    closestCluster = cluster;
                }
            }
            // insert tuple into B+tree by computing iDistance from closest cluster
            bPlusTree.insert(closestCluster.getI() * c + distance.calculate(closestCluster.getCentroid(), tupleWrapper.getCoordinates(tuple)), tuple);
            grid.setBPlusTree(bPlusTree);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("cubeId", "lat", "long", "alt", "text"));
    }
}
