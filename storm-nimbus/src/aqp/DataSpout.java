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
        // Generate random latitude, longitude, and text values
        double latitude = generateRandomCoordinate(-90, 90);
        double longitude = generateRandomCoordinate(-180, 180);
        double altitude = generateRandomAltitiude();
        String text = generateRandomText();

        // Emit the tuple with fields lat, long, text
        collector.emit(new Values(latitude, longitude, altitude, text));

        // Sleep for a short duration to control the emitting rate (adjust as needed)
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // Declare the output fields for the emitted tuple
        declarer.declare(new Fields("lat", "long", "alt", "text"));
    }

    private double generateRandomCoordinate(double min, double max) {
        return min + (max - min) * random.nextDouble();
    }

    private double generateRandomAltitiude() {
        return 10000;
    }

    private String generateRandomText() {
        // Replace this with your logic for generating random text
        String[] texts = {"Hello", "Storm", "Spout", "Tuple", "Apache"};
        return texts[random.nextInt(texts.length)];
    }
}
