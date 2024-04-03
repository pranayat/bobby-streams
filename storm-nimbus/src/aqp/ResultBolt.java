package aqp;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.Files;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

public class ResultBolt extends BaseRichBolt {

  OutputCollector _collector;
  private BufferedWriter joinTuplesWriter;
  private BufferedWriter joinAggregationWriter;

  @Override
  public void prepare(
          Map map,
          TopologyContext topologyContext,
          OutputCollector collector) {
      _collector = collector;

      Path projectRoot = Paths.get("").toAbsolutePath();
        
      // Specify the relative path from the project root to your file
      String joinTuplesFileName = "join_tuples.txt";
      String joinAggregationFileName = "join_aggregates.txt";
      
      // Construct the absolute file path
      String joinTuplesFilePath = projectRoot.resolve(joinTuplesFileName).toString();
      String joinAggregationFilePath = projectRoot.resolve(joinAggregationFileName).toString();

      Path joinTuplesFile = Paths.get(joinTuplesFilePath);
      Path joinAggregationFile = Paths.get(joinAggregationFilePath);
      try {
        if (!Files.exists(joinTuplesFile)) {
          Files.createFile(joinTuplesFile);
        }

        if (!Files.exists(joinAggregationFile)) {
          Files.createFile(joinAggregationFile);
        }
    
        joinTuplesWriter = new BufferedWriter(new FileWriter(joinTuplesFilePath, true));
        joinAggregationWriter = new BufferedWriter(new FileWriter(joinAggregationFilePath, true));
      } catch (IOException e) {
        e.printStackTrace();
      }
  }

  @Override
  public void execute(Tuple tuple) {
    
    // Write tuple values to the file
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < tuple.size(); i++) {
        sb.append(tuple.getValue(i)).append(",");
    }
    // Remove the last comma
    sb.deleteCharAt(sb.length() - 1);
    // Append a newline character
    sb.append("\n");
    try {
      if (tuple.getSourceStreamId().contains("-aggregateResultStream")){
        joinAggregationWriter.write(sb.toString());
        joinAggregationWriter.flush();
      } else {
        joinTuplesWriter.write(sb.toString());
        joinTuplesWriter.flush();
      }
    } catch (IOException e) {
      e.printStackTrace();
    }

    _collector.ack(tuple);
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer ofd) {
  }
}