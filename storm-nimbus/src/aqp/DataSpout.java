package aqp;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

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

        collector.emit("stream_1", new Values(UUID.randomUUID().toString(), latitude, longitude, altitude, text));
        collector.emit("stream_2", new Values(UUID.randomUUID().toString(), latitude, longitude, altitude, text));
        collector.emit("stream_3", new Values(UUID.randomUUID().toString(), latitude, longitude, altitude, text));

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        for (Stream stream : this.schemaConfig.getStreams()) {
            declarer.declareStream(stream.getId(), new Fields(stream.getFieldNames()));
        }
    }

    private double generateRandomCoordinate(double min, double max) {
        return min + (max - min) * random.nextDouble();
    }

    private Double generateRandomDouble() {
        Double[] texts = {1.0, 2.0, 3.0, 4.0, 5.0};
        return texts[random.nextInt(texts.length)];
    }
}
