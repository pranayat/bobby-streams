package aqp;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

public class NoResultBolt extends BaseBasicBolt {

  @Override
  public void execute(Tuple tuple, BasicOutputCollector collector) {
      // this bolt is only for acking unjoined tuples from JoinerBoltExact
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer ofd) {
  }
}