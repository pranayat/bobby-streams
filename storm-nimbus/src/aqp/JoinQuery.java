package aqp;

import org.apache.storm.tuple.Tuple;

import java.util.*;
import java.util.stream.Collectors;

public class JoinQuery {
    String id;
    double radius;
    List<String> streamIds;
    List<String> fields;
    List<List<Tuple>> results;
    Distance distance;
    IDistance iDistance;
    Panakos panakosSumSketch;
    String sumStream;
    String sumField;
    Panakos panakosCountSketch;

    public JoinQuery(String id, double radius, List<String> streamIds, List<String> fields, Distance distance, IDistance iDistance) {
        Collections.sort(fields);
        this.id = id;
        this.radius = radius;
        this.streamIds = streamIds;
        this.fields = fields;
        this.results = new ArrayList<>();
        this.distance = distance;
        this.iDistance = iDistance;
        this.panakosCountSketch = new Panakos();
        this.panakosSumSketch = new Panakos();
    }

    public Panakos getPanakosCountSketch() {
        return this.panakosCountSketch;
    }

    public Panakos getPanakosSumSketch() {
        return this.panakosSumSketch;
    }
    
    public void setSumStream(String sumStream) {
        this.sumStream = sumStream;
    }

    public void setSumField(String sumField) {
        this.sumField = sumField;
    }

    public String getSumStream() {
        return this.sumStream;
    }

    public String getSumField() {
        return this.sumField;
    }

    public String getId() {
        return this.id;
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

    private List<Tuple> findJoinPartnersInStreamNoIndex(Tuple tuple, QueryGroup queryGroup, List<Tuple> window, String streamToJoin) {
        TupleWrapper tupleWrapper = new TupleWrapper(queryGroup.getAxisNamesSorted());
        List<Tuple> joinCandidatesFromOtherStreams = new ArrayList<>();
        for (Tuple joinCandidate : window) {
            if (streamToJoin.equals(joinCandidate.getStringByField("streamId"))) {
                if (this.distance.calculate(tupleWrapper.getCoordinates(tuple, this.distance instanceof CosineDistance),
                tupleWrapper.getCoordinates(joinCandidate, this.distance instanceof CosineDistance)) <= this.getRadius()) {
                    joinCandidatesFromOtherStreams.add(joinCandidate);
                }
            }
        }

        return joinCandidatesFromOtherStreams;
    }

    private List<Tuple> findJoinPartnersInStream(Tuple tuple, QueryGroup queryGroup, String streamToJoin) {
        BPlusTreeNew<Double, Tuple> bPlusTree = queryGroup.getBPlusTree();
        TupleWrapper tupleWrapper = new TupleWrapper(queryGroup.getAxisNamesSorted());
        List<Tuple> joinCandidates = new ArrayList<>();

        // only need to search for join partners within a cluster as everything joinable is already inside it
        Cluster cluster = queryGroup.getCluster(tuple.getStringByField("clusterId"));
        IDistance iDistance = queryGroup.getIDistance();
        List<Double> searchBounds = iDistance.getSearchBounds(cluster.getRadius(), cluster.getCentroid(),
                cluster.getI(), queryGroup.getC(), this.getRadius(), tupleWrapper.getCoordinates(tuple, this.distance instanceof CosineDistance));

        if (!Double.isNaN(searchBounds.get(0)) && !Double.isNaN(searchBounds.get(1))) {
            joinCandidates.addAll(bPlusTree.searchRange(searchBounds.get(0), BPlusTreeNew.RangePolicy.INCLUSIVE, searchBounds.get(1), BPlusTreeNew.RangePolicy.INCLUSIVE));
        }

        List<Tuple> joinCandidatesFromOtherStreams = new ArrayList<>();
        for (Tuple joinCandidate : joinCandidates) {
            if (streamToJoin.equals(joinCandidate.getStringByField("streamId"))) {
                // iDistance has false positives as the index is simply distance from cluster center
                boolean isFalsePositive = this.distance.calculate(tupleWrapper.getCoordinates(tuple, this.distance instanceof CosineDistance),
                        tupleWrapper.getCoordinates(joinCandidate, this.distance instanceof CosineDistance)) > this.getRadius();

                if (!isFalsePositive) {
                    joinCandidatesFromOtherStreams.add(joinCandidate);
                } else {
                    // TODO collect some stats here for iDistance false positives
                }
            }
        }

        return joinCandidatesFromOtherStreams;
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
                .filter(s -> !s.equals(tuple.getStringByField("streamId"))).collect(Collectors.toList()); // [b, c]

        Map<Tuple, List<Tuple>> joinPartnersForTuple = new HashMap<>();
        joinPartnersForTuple.put(tuple, new ArrayList<>()); // { a1: [] }
        Map<String, Map<Tuple, List<Tuple>>> joinPartnersByStream = new HashMap<>();
        joinPartnersByStream.put(tuple.getStringByField("streamId"), joinPartnersForTuple); // { a: { a1: [] } }

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

    public List<Tuple> executeNoIndex(Tuple tuple, QueryGroup queryGroup, List<Tuple> window) {
        List<String> unjoinedStreams = this.streamIds.stream()
                .filter(s -> !s.equals(tuple.getStringByField("streamId"))).collect(Collectors.toList()); // [b, c]

        Map<Tuple, List<Tuple>> joinPartnersForTuple = new HashMap<>();
        joinPartnersForTuple.put(tuple, new ArrayList<>()); // { a1: [] }
        Map<String, Map<Tuple, List<Tuple>>> joinPartnersByStream = new HashMap<>();
        joinPartnersByStream.put(tuple.getStringByField("streamId"), joinPartnersForTuple); // { a: { a1: [] } }

        for (String streamToJoin : unjoinedStreams) { // c

            for (Map.Entry<String, Map<Tuple, List<Tuple>>> entry : joinPartnersByStream.entrySet()) {

                String leftStream = entry.getKey(); // b
                Map<Tuple, List<Tuple>> leftStreamTuplePartnersMap = entry.getValue(); // { b1: [], b2: [], b3: [], b4: [] }

                for (Map.Entry<Tuple, List<Tuple>> leftStreamTuplePartnersPair : leftStreamTuplePartnersMap.entrySet()) { // b1: []
                    Tuple leftTuple = leftStreamTuplePartnersPair.getKey(); // b1
                    List<Tuple> joinPartners = this.findJoinPartnersInStreamNoIndex(leftTuple, queryGroup, window, streamToJoin);
                    joinPartnersByStream.get(leftStream).put(leftTuple, joinPartners); // { b: { b1: [c1, c2] } }
                }
            }

            Map<String, List<Tuple>> joinPartnerSetByStream = this.collectJoinPartnerSetByStream(joinPartnersByStream); // { a: [c1, c2, c3], b: [c1, c2, c4] }
            List<Tuple> commonJoinPartnersAcrossStreams = this.findIntersectionAcrossStreams(joinPartnerSetByStream); // [c1, c2]
            joinPartnersByStream = this.keepCliqueTuplesOnly(joinPartnersByStream, commonJoinPartnersAcrossStreams); // containing c1 or c2
            joinPartnersByStream = this.clearJoinPartners(joinPartnersByStream);
            joinPartnersByStream = this.addIntersection(joinPartnersByStream, streamToJoin, commonJoinPartnersAcrossStreams);
        }

        return this.flattenJoinMap(joinPartnersByStream);
    }
}
