package aqp;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;
import java.util.Random;

public class DataSpout extends BaseRichSpout {
    private SpoutOutputCollector collector;
    private Random random;

    @Override
    public void open(Map<String, Object> config, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        this.random = new Random();
    }

    @Override
    public void nextTuple() {
        double latitude = generateRandomCoordinate(-90, 90);
        double longitude = generateRandomCoordinate(-180, 180);
        double altitude = generateRandomAltitude();
        String text = generateRandomText();

        collector.emit("stream_1", new Values(latitude, longitude, altitude, text));
        collector.emit("stream_2", new Values(latitude, longitude, altitude, text));

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("stream_1", new Fields("lat", "long", "alt", "text"));
        declarer.declareStream("stream_2", new Fields("lat", "long", "alt", "text"));
    }

    private double generateRandomCoordinate(double min, double max) {
        return min + (max - min) * random.nextDouble();
    }

    private double generateRandomAltitude() {
        return 10000;
    }

    private String generateRandomText() {
        String[] texts = {"Hello", "Storm", "Spout", "Tuple", "Apache"};
        return texts[random.nextInt(texts.length)];
    }
}
