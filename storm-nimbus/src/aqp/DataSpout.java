package aqp;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

public class DataSpout extends BaseRichSpout {
    private SpoutOutputCollector collector;
    private Random random;
    private SchemaConfig schemaConfig;
    private String streamId;

    public DataSpout(String streamId) {
        this.schemaConfig = SchemaConfigBuilder.build();
        this.streamId = streamId;
    }

    @Override
    public void open(Map<String, Object> stormConfig, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        this.random = new Random();
    }

    @Override
    public void nextTuple() {

        String id = "";
        if (this.streamId.equals("stream_1")) {
            id = UUID.randomUUID().toString();
            collector.emit(new Values(id, 20.0, 80.0, 100.0, "stream_1"), id); // T1
            id = UUID.randomUUID().toString();
            collector.emit(new Values(id, 40.0, 60.0, 100.0, "stream_1"), id); // T4
            id = UUID.randomUUID().toString();
            collector.emit(new Values(id, 60.0, 30.0, 100.0, "stream_1"), id); // T6
            id = UUID.randomUUID().toString();
            collector.emit(new Values(id, 80.0, 20.0, 100.0, "stream_1"), id); // T8
        } else if (this.streamId.equals("stream_2")) {
            id = UUID.randomUUID().toString();
            collector.emit(new Values(id, 20.0, 70.0, 100.0, "stream_2"), id); // T2
            id = UUID.randomUUID().toString();
            collector.emit(new Values(id, 60.0, 40.0, 100.0, "stream_2"), id); // T5
        } else {
            id = UUID.randomUUID().toString();
            collector.emit(new Values(id, 30.0, 70.0, 100.0, "stream_3"), id); // T3
            id = UUID.randomUUID().toString();
            collector.emit(new Values(id, 70.0, 30.0, 100.0, "stream_3"), id); // T7
        }

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        Stream stream = this.schemaConfig.getStreams().get(0);
        List<String> fields = new ArrayList<String>(stream.getFieldNames());
        fields.add("streamId");
        declarer.declare(new Fields(fields));
    }

    private double generateRandomCoordinate(double min, double max) {
        return min + (max - min) * random.nextDouble();
    }
}
