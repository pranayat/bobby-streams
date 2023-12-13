package aqp;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

public class CubeBolt extends BaseRichBolt {
    OutputCollector _collector;

    @Override
    public void prepare(
            Map map,
            TopologyContext topologyContext,
            OutputCollector collector) {
        _collector = collector;

//        this.joinQueryCache = JoinQueryCache.getInstance();
    }

    @Override
    public void execute(Tuple input) {
        System.out.println("cubeee " + input);
//        if (input.getSourceComponent().equals("data") && this.joinQueryCache.size() > 0) {
//        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("tupleType", "streamId", "cubeId", "lat", "long", "alt", "text"));
    }
}
