package aqp;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

public class GridBolt extends BaseRichBolt {
    OutputCollector _collector;

    @Override
    public void prepare(
        Map                     map,
        TopologyContext         topologyContext,
        OutputCollector         collector)
    {
      _collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        // Extract values from the tuple using field names
        double lat = input.getDoubleByField("lat");
        double lon = input.getDoubleByField("long");
        double alt = input.getDoubleByField("alt");
        String text = input.getStringByField("text");

        // Print the values
        System.out.println("Received tuple - Lat: " + lat + ", Long: " + lon + ", Alt: " + alt + ", Text: " + text);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // This bolt doesn't emit any tuples
    }
}
