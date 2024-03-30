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
    Boolean once;

    public DataSpout(String streamId) {
        this.schemaConfig = SchemaConfigBuilder.build();
        this.streamId = streamId;
        this.once = false;
    }

    @Override
    public void open(Map<String, Object> stormConfig, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        this.random = new Random();
    }

    @Override
    public void nextTuple() {

        if(this.once) {
            return;
        }

        if (this.streamId.equals("stream_1")) {
            collector.emit(new Values("T1_S1", 10.0, 70.0, 100.0, 2300.0, "stream_1"), "T1_S1"); // T1
            collector.emit(new Values("T4_S1", 30.0, 50.0, 100.0, 2300.0, "stream_1"), "T4_S1"); // T4
            collector.emit(new Values("T6_S1", 50.0, 20.0, 100.0, 2200.0, "stream_1"), "T6_S1"); // T6
            collector.emit(new Values("T8_S1", 70.0, 10.0, 100.0, 2200.0, "stream_1"), "T8_S1"); // T8
        } else if (this.streamId.equals("stream_2")) {
            collector.emit(new Values("T2_S2", 10.0, 60.0, 100.0, 2100.0, "stream_2"), "T2_S2"); // T2
            collector.emit(new Values("T5_S2", 50.0, 30.0, 100.0, 2100.0, "stream_2"), "T5_S2"); // T5
        } else {
            collector.emit(new Values("T3_S3", 20.0, 60.0, 100.0, 2000.0, "stream_3"), "T3_S3"); // T3
            collector.emit(new Values("T7_S3", 60.0, 20.0, 100.0, 2000.0, "stream_3"), "T7_S3"); // T7
        }

        this.once = true;

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
