package aqp;

import org.apache.storm.tuple.Tuple;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class JoinQuery {
    int radius;
    List<String> streamIds;
    List<String> fields;
    List<List<Tuple>> results;

    enum QuerySpherePosition {
        INSIDE,
        INTERSECTS,
        OUTSIDE
    }

    public JoinQuery(int radius, List<String> streamIds, List<String> fields) {
        Collections.sort(fields);
        this.radius = radius;
        this.streamIds = streamIds;
        this.fields = fields;
        this.results = new ArrayList<>();
    }

    public int getRadius() {
        return this.radius;
    }

    public List<String> getStreamIds() {
        return this.streamIds;
    }

    public List<String> getFieldsSorted() {
        Collections.sort(this.fields);
        return this.fields;
    }

    private QuerySpherePosition getQuerySpherePosition(double queryTupleToCentroidDistance, double clusterRadius) {
        if (queryTupleToCentroidDistance < clusterRadius) {
            return QuerySpherePosition.INSIDE;
        } else if (queryTupleToCentroidDistance < clusterRadius + this.getRadius()) {
            return QuerySpherePosition.INTERSECTS;
        } else {
            return QuerySpherePosition.OUTSIDE;
        }
    }

    private List<Tuple> findJoinPartnersInStream(Tuple tuple, Grid grid, String streamToJoin) {
        BPlusTree bPlusTree = grid.getBPlusTree();
        TupleWrapper tupleWrapper = new TupleWrapper(grid.getAxisNamesSorted());
        Distance distance = new EuclideanDistance();
        List<Tuple> joinCandidates = new ArrayList<>();

        for (Cluster cluster : grid.getClusters()) {
            double queryTupleToCentroidDistance = distance.calculate(cluster.getCentroid(), tupleWrapper.getCoordinates(tuple));

            switch (this.getQuerySpherePosition(queryTupleToCentroidDistance, cluster.getRadius())) {
                case INSIDE:
                    joinCandidates.addAll(bPlusTree.search(cluster.getI() * grid.getC() + queryTupleToCentroidDistance - this.getRadius(),
                            Math.min(cluster.getI() * grid.getC() + cluster.getRadius(), cluster.getI() * grid.getC() + queryTupleToCentroidDistance + this.getRadius())));
                case INTERSECTS:
                    joinCandidates.addAll(bPlusTree.search(cluster.getI() * grid.getC() + queryTupleToCentroidDistance - this.getRadius(),
                            cluster.getI() * grid.getC() + cluster.getRadius()));
            }
        }

        // remove join partners found in the same stream to prevent within stream joins
        List<Tuple> joinCandidatesFromOtherStreams = new ArrayList<>();
        for (Tuple joinCandidate : joinCandidates) {
            if (streamToJoin.equals(joinCandidate.getSourceStreamId())) {
                joinCandidatesFromOtherStreams.add(joinCandidate);
            }
        }

        return joinCandidatesFromOtherStreams;
    }

    private List<Tuple> findIntersection(List<List<Tuple>> listOfTupleLists) {
        List<Tuple> result = new ArrayList<>();
        for (List<Tuple> tupleList : listOfTupleLists) {
            if (result.size() == 0) {
                result.addAll(tupleList);
            } else {
                result.retainAll(tupleList);
            }
        }

        return result;
    }

    public List<Tuple> execute(Tuple tuple, Grid grid) {
        // remove tuple's stream to prevent joins within streams
        List<String> unjoinedStreams = this.streamIds.stream()
                .filter(s -> !s.equals(tuple.getSourceStreamId())).collect(Collectors.toList());

        List<Tuple> leftTuplesToJoin = new ArrayList<>();
        leftTuplesToJoin.add(tuple);
        // initialized with [A1], find partners for [A1] in stream B
        while (unjoinedStreams.size() > 0) {
            // join stream C now (assuming already joined [A1] with stream B in previous loop to get [A1, B1, B2])
            String streamToJoin = unjoinedStreams.remove(unjoinedStreams.size() - 1);
            List<List<Tuple>> partnerTuplesPerLeftTuple = new ArrayList<>();
            // [A1, B1, B2] will be joined with tuples in stream C
            for (Tuple leftTuple : leftTuplesToJoin) {
                // we found [C1, C2], [C1, C3], [C1, C4] join partners for A1, B1 and B2 respectively
                List<Tuple> joinPartners = this.findJoinPartnersInStream(leftTuple, grid, streamToJoin);
                // every single tuple A1, B1, B2 should find a join partner in stream C, else abort and return early
                if (joinPartners.size() == 0) {
                    return new ArrayList<>();
                }
                partnerTuplesPerLeftTuple.add(joinPartners);
            }

            // C1 is common across join partners [C1, C2], [C1, C3], [C1, C4] of tuples A1, B1 and B2
            List<Tuple> commonJoinPartners = this.findIntersection(partnerTuplesPerLeftTuple);
            if (commonJoinPartners.size() == 0) {
                // if no common join partners then abort and return
                return new ArrayList<>();
            }
            // these join results will now be joined with the remaining streams
            // eg. [A1, B1, B2, C1] will join tuples from stream D next
            leftTuplesToJoin.addAll(commonJoinPartners);
        }

        // we found join partners [B1, B2, B3, C1, D1, D2, D3, D4] for tuple A1
        // add [A1, B1, B2, B3, C1, C2, D1, D2, D3, D4] to query result
        // in result, the first tuple A1 will be the tuple in the bolt window currently being joined, the rest will be the found join partners in other streams
        return leftTuplesToJoin;
    }
}
