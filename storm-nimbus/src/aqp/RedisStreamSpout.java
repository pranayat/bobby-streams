package aqp;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.params.XReadGroupParams;
import redis.clients.jedis.resps.StreamEntry;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RedisStreamSpout extends BaseRichSpout {
    private SpoutOutputCollector collector;
    private Jedis jedis;
    ObjectMapper objectMapper;
    SchemaConfig schemaConfig;
    String streamId;
    String lastId = null;

    public RedisStreamSpout() {
        this.schemaConfig = SchemaConfigBuilder.build();
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        this.objectMapper = new ObjectMapper();

        this.jedis = new Jedis("redis", 6379); // Set your Redis server details
    }

    @Override
    public void nextTuple() {        
        List<StreamEntry> entries = null;
        try {
            if (this.lastId == null) {
                entries = jedis.xrange("flight_stream", "-", "+", 20);
            } else {
                entries = jedis.xrange("flight_stream", "(" + lastId, "+", 20);
            }

            if (entries != null) {
                lastId = entries.get(entries.size() - 1).getID().toString();
            }
        } catch (Exception e) {
            // System.err.println(e);
        }

        if (entries == null) {
            return;
        }

        for (StreamEntry entry : entries) {
            String jsonFields = entry.getFields().get("data");
            JsonNode jsonNode;
            try {
                jsonNode = objectMapper.readTree(jsonFields);
                List<Object> values = new ArrayList<>();
                for (Field field : this.schemaConfig.getStreamById("stream_1").getFields()) {
                    if (field.getName().equals("tupleId")) {
                        values.add(entry.getID().toString());
                    } else if (field.getType().equals("double")) {
                        values.add(jsonNode.get(field.getName()).asDouble());
                    } else {
                        values.add(jsonNode.get(field.getName()).asText());
                    }
                }
    
                values.add(jsonNode.get("streamId").asText());
                collector.emit(new Values(values.toArray()), entry.getID().toString());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        Stream stream = this.schemaConfig.getStreams().get(0);
        List<String> fields = new ArrayList<String>(stream.getFieldNames());
        fields.add("streamId");
        declarer.declare(new Fields(fields));
    }

//    @Override
//    public void ack(Object msgId) {
    // Acknowledgment logic (if needed)
//    }

//    @Override
//    public void fail(Object msgId) {
    // Failure handling logic (if needed)
//    }

    @Override
    public void close() {
        jedis.close();
    }
}
