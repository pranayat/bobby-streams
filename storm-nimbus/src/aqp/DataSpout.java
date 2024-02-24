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

    public DataSpout() {
        this.schemaConfig = SchemaConfigBuilder.build();
    }

    @Override
    public void open(Map<String, Object> stormConfig, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        this.random = new Random();
    }

    @Override
    public void nextTuple() {
        double latitude = generateRandomCoordinate(10000, 50000);
        double longitude = generateRandomCoordinate(1000, 50000);
        double altitude = generateRandomCoordinate(1000, 50000);
        double text = generateRandomDouble();

        // for a join radius of 2300 only A1, B1, C1 should join
        collector.emit(new Values("stream_1", UUID.randomUUID().toString(), 0.0, 0.0, 0.0, text)); // A1
        collector.emit(new Values("stream_2", UUID.randomUUID().toString(), 2000.0, 0.0, 0.0, text)); // B1
        collector.emit(new Values("stream_3", UUID.randomUUID().toString(), 0.0, 1000.0, 0.0, text)); // C1

        collector.emit(new Values("stream_2", UUID.randomUUID().toString(), 3000.0, 0.0, 0.0, text));
        collector.emit(new Values("stream_2", UUID.randomUUID().toString(), 0.0, 3000.0, 0.0, text));

        collector.emit(new Values("stream_3", UUID.randomUUID().toString(), 0.0, 2000.0, 3000.0, text));
        collector.emit(new Values("stream_3", UUID.randomUUID().toString(), 3000.0, 2000.0, 0.0, text));

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

    private Double generateRandomDouble() {
        Double[] texts = { 1.0, 2.0, 3.0, 4.0, 5.0 };
        return texts[random.nextInt(texts.length)];
    }
}
