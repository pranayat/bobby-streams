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
  private BufferedWriter writer;

  @Override
  public void prepare(
          Map map,
          TopologyContext topologyContext,
          OutputCollector collector) {
      _collector = collector;

      Path projectRoot = Paths.get("").toAbsolutePath();
        
      // Specify the relative path from the project root to your file
      String relativePath = "output.txt";
      
      // Construct the absolute file path
      String absolutePath = projectRoot.resolve(relativePath).toString();

      Path file = Paths.get(absolutePath);
      try {
        if (!Files.exists(file)) {
          Files.createFile(file);
        }
    
        writer = new BufferedWriter(new FileWriter(absolutePath, true));
      } catch (IOException e) {
        // TODO Auto-generated catch block
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
      writer.write(sb.toString());
      // Flush to ensure the content is written immediately
      writer.flush();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    _collector.ack(tuple);
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer ofd) {
  }
}