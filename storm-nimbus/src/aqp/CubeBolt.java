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

    private Grid getTupleGrid(Tuple tuple) {
        return this.grids.stream()
                .filter(g -> g.getName().equals(tuple.getStringByField("gridName")))
                .findFirst()
                .get();
    }

    public Boolean isTupleInGridTree(Tuple tuple, Grid grid) {
        Distance distance = new EuclideanDistance();
        TupleWrapper tupleWrapper = new TupleWrapper(grid.getAxisNamesSorted());
        BPlusTree bPlusTree = grid.getBPlusTree();

        for (Cluster cluster : grid.getClusters()) {
            double queryTupleToCentroidDistance = distance.calculate(cluster.getCentroid(), tupleWrapper.getCoordinates(tuple));

            // check for duplicates since we replicate tuples to adjacent cells
            List<Tuple> existingTuples = bPlusTree.search(cluster.getI() * grid.getC() + queryTupleToCentroidDistance,
                    cluster.getI() * grid.getC() + queryTupleToCentroidDistance);
            // multiple tuples with different ids could have the same iDistance so check the tupleID as well
            if (existingTuples.stream().anyMatch(t -> t.getStringByField("tupleId").equals(tuple.getStringByField("tupleId")))) {
                return true;
            }
        }

        return false;
    }

    private void insertIntoGridTree(Tuple tuple, Grid grid) {
        Distance distance = new EuclideanDistance();
        Cluster closestCluster = null;
        double closestClusterDistance = -1;
        TupleWrapper tupleWrapper = new TupleWrapper(grid.getAxisNamesSorted());
        BPlusTree bPlusTree = grid.getBPlusTree();

        for (Cluster cluster : grid.getClusters()) {
            double queryTupleToCentroidDistance = distance.calculate(cluster.getCentroid(), tupleWrapper.getCoordinates(tuple));

            if (closestClusterDistance == -1 || queryTupleToCentroidDistance < closestClusterDistance) {
                closestClusterDistance = queryTupleToCentroidDistance;
                closestCluster = cluster;
            }
        }

        // insert tuple into B+tree by computing iDistance from closest cluster
        bPlusTree.insert(closestCluster.getI() * grid.getC() + closestClusterDistance, tuple);
        grid.setBPlusTree(bPlusTree);
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

        for (Tuple tuple : inputWindow.get()) {
            Grid grid = this.getTupleGrid(tuple);

            if (this.isTupleInGridTree(tuple, grid)) {
                // already in tree and also hence already called joinQuery.execute on it
                continue;
            }

            for (JoinQuery joinQuery : grid.getJoinQueries()) {
                joinQuery.execute(tuple, grid);
                System.out.println(joinQuery);
            }

            // index the tuple
            this.insertIntoGridTree(tuple, grid);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("cubeId", "lat", "long", "alt", "text"));
    }
}
