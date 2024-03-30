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

public class Test1Spout extends BaseRichSpout {
    private SpoutOutputCollector collector;
    private Random random;
    private SchemaConfig schemaConfig;
    private String streamId;
    Boolean once;

    public Test1Spout(String streamId) {
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
            collector.emit(new Values("T1_S1", 10.0, 70.0, 100.0, 2100.0, "stream_1"), "T1_S1");
            collector.emit(new Values("T2_S1", 30.0, 50.0, 100.0, 2100.0, "stream_1"), "T2_S1");
            collector.emit(new Values("T3_S1", 50.0, 20.0, 100.0, 2100.0, "stream_1"), "T3_S1");
            collector.emit(new Values("T4_S1", 70.0, 10.0, 100.0, 2100.0, "stream_1"), "T4_S1");

            collector.emit(new Values("T5_S1", 10.0, 70.0, 100.0, 2100.0, "stream_1"), "T5_S1");
            collector.emit(new Values("T6_S1", 30.0, 50.0, 100.0, 2100.0, "stream_1"), "T6_S1");
            collector.emit(new Values("T7_S1", 50.0, 20.0, 100.0, 2100.0, "stream_1"), "T7_S1");
            collector.emit(new Values("T8_S1", 70.0, 10.0, 100.0, 2100.0, "stream_1"), "T8_S1");

            collector.emit(new Values("T9_S1", 10.0, 70.0, 100.0, 2200.0, "stream_1"), "T9_S1"); 
            collector.emit(new Values("T10_S1", 30.0, 50.0, 100.0, 2200.0, "stream_1"), "T10_S1");
            collector.emit(new Values("T11_S1", 50.0, 20.0, 100.0, 2200.0, "stream_1"), "T11_S1");
            collector.emit(new Values("T12_S1", 70.0, 10.0, 100.0, 2200.0, "stream_1"), "T12_S1");

            collector.emit(new Values("T13_S1", 10.0, 70.0, 100.0, 2200.0, "stream_1"), "T13_S1");
            collector.emit(new Values("T14_S1", 30.0, 50.0, 100.0, 2200.0, "stream_1"), "T14_S1");
            collector.emit(new Values("T15_S1", 50.0, 20.0, 100.0, 2200.0, "stream_1"), "T15_S1");
            collector.emit(new Values("T16_S1", 70.0, 10.0, 100.0, 2200.0, "stream_1"), "T16_S1");
        } else if (this.streamId.equals("stream_2")) {
            collector.emit(new Values("T17_S2", 10.0, 60.0, 100.0, 2200.0, "stream_2"), "T17_S2");
            collector.emit(new Values("T18_S2", 50.0, 30.0, 100.0, 2300.0, "stream_2"), "T18_S2");

            collector.emit(new Values("T19_S2", 10.0, 60.0, 100.0, 2300.0, "stream_2"), "T19_S2");
            collector.emit(new Values("T20_S2", 50.0, 30.0, 100.0, 2300.0, "stream_2"), "T20_S2");

            collector.emit(new Values("T21_S2", 10.0, 60.0, 100.0, 2100.0, "stream_2"), "T21_S2");
            collector.emit(new Values("T22_S2", 50.0, 30.0, 100.0, 2100.0, "stream_2"), "T22_S2");

            collector.emit(new Values("T23_S2", 10.0, 60.0, 100.0, 2100.0, "stream_2"), "T23_S2");
            collector.emit(new Values("T24_S2", 50.0, 30.0, 100.0, 2100.0, "stream_2"), "T24_S2");
        } else {
            collector.emit(new Values("T25_S3", 20.0, 60.0, 100.0, 2100.0, "stream_3"), "T25_S3");
            collector.emit(new Values("T26_S3", 60.0, 20.0, 100.0, 2100.0, "stream_3"), "T26_S3");

            collector.emit(new Values("T27_S3", 20.0, 60.0, 100.0, 2100.0, "stream_3"), "T27_S3");
            collector.emit(new Values("T28_S3", 60.0, 20.0, 100.0, 2100.0, "stream_3"), "T28_S3");

            collector.emit(new Values("T29_S3", 20.0, 60.0, 100.0, 2100.0, "stream_3"), "T29_S3");
            collector.emit(new Values("T30_S3", 60.0, 20.0, 100.0, 2100.0, "stream_3"), "T30_S3");

            collector.emit(new Values("T31_S3", 20.0, 60.0, 100.0, 2100.0, "stream_3"), "T31_S3");
            collector.emit(new Values("T32_S3", 60.0, 20.0, 100.0, 2100.0, "stream_3"), "T32_S3");
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
