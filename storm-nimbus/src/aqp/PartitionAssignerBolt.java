package aqp;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class PartitionAssignerBolt extends BaseRichBolt {
    OutputCollector _collector;
    private SchemaConfig schemaConfig;
    List<JoinQuery> joinQueries;

    public PartitionAssignerBolt() {
        this.schemaConfig = SchemaConfigBuilder.build();
    }

    @Override
    public void prepare(
            Map stormConfig,
            TopologyContext topologyContext,
            OutputCollector collector) {
        _collector = collector;

    }

    @Override
    public void execute(Tuple input) {
        String tupleStreamId = input.getSourceStreamId();
        // TODO emit by partitionId; instead of shuffle grouping (clusterId, queryGroupName) assign these to partitions
        _collector.emit(tupleStreamId, input.getValues());
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

        for (Stream stream : this.schemaConfig.getStreams()) {
            // different streams may have different fields
            List<String> fields = new ArrayList<String>(stream.getFieldNames());
            fields.add("clusterId");
            fields.add("queryGroupName");
            declarer.declareStream(stream.getId(), new Fields(fields));
        }
    }
}
