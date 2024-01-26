package aqp;

import org.apache.storm.tuple.Tuple;

import java.util.*;
import java.util.stream.Collectors;

public class JoinQuery {
    double radius;
    List<String> streamIds;
    List<String> fields;
    List<List<Tuple>> results;
    Distance distance;
    IDistance iDistance;

    public JoinQuery(double radius, List<String> streamIds, List<String> fields, Distance distance, IDistance iDistance) {
        Collections.sort(fields);
        this.radius = radius;
        this.streamIds = streamIds;
        this.fields = fields;
        this.results = new ArrayList<>();
        this.distance = distance;
        this.iDistance = iDistance;
    }

    public double getRadius() {
        return this.radius;
    }

    public List<String> getStreamIds() {
        return this.streamIds;
    }

    public List<String> getFieldsSorted() {
        Collections.sort(this.fields);
        return this.fields;
    }

    public Distance getDistance() {
        return this.distance;
    }

    public IDistance getIDistance() {
        return this.iDistance;
    }

    private List<Tuple> findJoinPartnersInStream(Tuple tuple, QueryGroup queryGroup, String streamToJoin) {
        BPlusTree bPlusTree = queryGroup.getBPlusTree();
        TupleWrapper tupleWrapper = new TupleWrapper(queryGroup.getAxisNamesSorted());
        List<Tuple> joinCandidates = new ArrayList<>();

        for (Cluster cluster : queryGroup.getClusterMap().values()) {
            IDistance iDistance = queryGroup.getIDistance();
            List<Double> searchBounds = iDistance.getSearchBounds(cluster.getRadius(), cluster.getCentroid(),
                    cluster.getI(), queryGroup.getC(), this.getRadius(), tupleWrapper.getCoordinates(tuple, this.distance instanceof CosineDistance));

            if (!Double.isNaN(searchBounds.get(0)) && !Double.isNaN(searchBounds.get(1))) {
                joinCandidates.addAll(bPlusTree.search(searchBounds.get(0), searchBounds.get(1)));
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

    private Map<String, Map<Tuple, List<Tuple>>> convertToNestedMap(Map<String, List<Tuple>> originalMap) {
        Map<String, Map<Tuple, List<Tuple>>> convertedMap = new HashMap<>();

        for (Map.Entry<String, List<Tuple>> entry : originalMap.entrySet()) {
            String key = entry.getKey();
            List<Tuple> values = entry.getValue();

            // Create a nested map for the current key
            Map<Tuple, List<Tuple>> nestedMap = new HashMap<>();
            for (Tuple value : values) {
                nestedMap.put(value, new ArrayList<>());
            }

            // Put the nested map into the converted map
            convertedMap.put(key, nestedMap);
        }

        return convertedMap;
    }

    private Map<String, List<Tuple>> collectJoinPartnerSetByStream(Map<String, Map<Tuple, List<Tuple>>> joinPartnersByStream) {
        Map<String, List<Tuple>> result = new HashMap<>();

        for (Map.Entry<String, Map<Tuple, List<Tuple>>> entry : joinPartnersByStream.entrySet()) {
            String key = entry.getKey();
            List<Tuple> values = entry.getValue().values().stream()
                    .flatMap(List::stream)
                    .collect(Collectors.toList());

            result.put(key, values); // TODO remove duplicate tuples in values
        }

        return result;
    }

    private List<Tuple> findIntersectionAcrossStreams(Map<String, List<Tuple>> joinPartnerSetByStream) {
        if (joinPartnerSetByStream == null || joinPartnerSetByStream.isEmpty()) {
            return Collections.emptyList();
        }

        Set<Tuple> commonElements = new HashSet<>(joinPartnerSetByStream.values().iterator().next());

        for (List<Tuple> values : joinPartnerSetByStream.values()) {
            commonElements.retainAll(new HashSet<>(values));
        }

        return new ArrayList<>(commonElements);
    }

    private Map<String, Map<Tuple, List<Tuple>>> keepCliqueTuplesOnly(Map<String, Map<Tuple, List<Tuple>>> joinPartnersByStream, List<Tuple> commonJoinPartnersAcrossStreams) {
        return joinPartnersByStream.entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> entry.getValue().entrySet().stream()
                                .filter(innerEntry -> innerEntry.getValue().stream().anyMatch(tuple -> commonJoinPartnersAcrossStreams.contains(tuple)))
                                .collect(Collectors.toMap(
                                        Map.Entry::getKey,
                                        Map.Entry::getValue
                                ))
                ));
    }

    private Map<String, Map<Tuple, List<Tuple>>> clearJoinPartners(Map<String, Map<Tuple, List<Tuple>>> joinPartnersByStream) {
        return joinPartnersByStream.entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> entry.getValue().keySet().stream()
                                .collect(Collectors.toMap(key -> key, key -> new ArrayList<>()))
                ));
    }

    private Map<String, Map<Tuple, List<Tuple>>> addIntersection(Map<String, Map<Tuple, List<Tuple>>> joinPartnersByStream, String streamToJoin, List<Tuple> commonJoinPartnersAcrossStreams) {
        joinPartnersByStream.put(streamToJoin, new HashMap<Tuple, List<Tuple>>());

        for (Tuple partner : commonJoinPartnersAcrossStreams) {
            joinPartnersByStream.get(streamToJoin).put(partner, new ArrayList<>());
        }

        return joinPartnersByStream;
    }

    private List<Tuple> flattenJoinMap(Map<String, Map<Tuple, List<Tuple>>> joinPartnersByStream) {
        List<Tuple> result = new ArrayList<>();

        for (Map.Entry<String, Map<Tuple, List<Tuple>>> entry : joinPartnersByStream.entrySet()) {
            Map<Tuple, List<Tuple>> innerMap = entry.getValue();
            result.addAll(innerMap.keySet());
        }

        return result;
    }

    public List<Tuple> execute(Tuple tuple, QueryGroup queryGroup) {
        List<String> unjoinedStreams = this.streamIds.stream()
                .filter(s -> !s.equals(tuple.getSourceStreamId())).collect(Collectors.toList()); // [b, c]

        Map<Tuple, List<Tuple>> joinPartnersForTuple = new HashMap<>();
        joinPartnersForTuple.put(tuple, new ArrayList<>()); // { a1: [] }
        Map<String, Map<Tuple, List<Tuple>>> joinPartnersByStream = new HashMap<>();
        joinPartnersByStream.put(tuple.getSourceStreamId(), joinPartnersForTuple); // { a: { a1: [] } }

        for (String streamToJoin : unjoinedStreams) { // c

            for (Map.Entry<String, Map<Tuple, List<Tuple>>> entry : joinPartnersByStream.entrySet()) {

                String leftStream = entry.getKey(); // b
                Map<Tuple, List<Tuple>> leftStreamTuplePartnersMap = entry.getValue(); // { b1: [], b2: [], b3: [], b4: [] }

                for (Map.Entry<Tuple, List<Tuple>> leftStreamTuplePartnersPair : leftStreamTuplePartnersMap.entrySet()) { // b1: []
                    Tuple leftTuple = leftStreamTuplePartnersPair.getKey(); // b1
                    List<Tuple> joinPartners = this.findJoinPartnersInStream(leftTuple, queryGroup, streamToJoin);
                    joinPartnersByStream.get(leftStream).put(leftTuple, joinPartners); // { b: { b1: [c1, c2] } }
                }
            }
            /*
            joinPartnersByStream = {
                a: {
                    a1: [c1, c2, c3]
                },
                b: {
                    b1: [c1, c2]
                    b2: [c2],
                    b3: [],
                    b4: [c4]
                }
            }
            */

            Map<String, List<Tuple>> joinPartnerSetByStream = this.collectJoinPartnerSetByStream(joinPartnersByStream); // { a: [c1, c2, c3], b: [c1, c2, c4] }
            List<Tuple> commonJoinPartnersAcrossStreams = this.findIntersectionAcrossStreams(joinPartnerSetByStream); // [c1, c2]
            joinPartnersByStream = this.keepCliqueTuplesOnly(joinPartnersByStream, commonJoinPartnersAcrossStreams); // containing c1 or c2
            /*
                {
                    a: {
                        a1: [c1, c2, c3]
                    },
                    b: {
                        b1: [c1, c2]
                        b2: [c2],
                    }
                }
             */
            joinPartnersByStream = this.clearJoinPartners(joinPartnersByStream);
            /*
                {
                    a: {
                        a1: []
                    },
                    b: {
                        b1: []
                        b2: [],
                    }
                }
             */
            joinPartnersByStream = this.addIntersection(joinPartnersByStream, streamToJoin, commonJoinPartnersAcrossStreams);
            /*
             * {
                a: {
                    a1: []
                },
                b: {
                    b1: []
                    b2: [],
                },
                c: {
                    c1: [],
                    c2: []
                }
               }
             */
        }

        return this.flattenJoinMap(joinPartnersByStream);
        /*
         * [a1, b1, b2, c1, c2]
         */
    }
}
