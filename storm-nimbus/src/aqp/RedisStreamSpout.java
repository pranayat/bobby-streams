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

    public RedisStreamSpout(String streamId) {
        this.schemaConfig = SchemaConfigBuilder.build();
        this.streamId = streamId;
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        this.objectMapper = new ObjectMapper();

        this.jedis = new Jedis("redis", 6379); // Set your Redis server details
        try {
            jedis.xgroupCreate(this.streamId, "storm_group", StreamEntryID.LAST_ENTRY, false);
            jedis.xgroupCreateConsumer(this.streamId, "storm_group", "storm_consumer");
        } catch (Exception e) {
            // Group already exists
            System.err.println(e);
        }
    }

    @Override
    public void nextTuple() {
        // Fetch data from Redis stream for all unreceived entries
        XReadGroupParams xReadGroupParams = new XReadGroupParams();
        xReadGroupParams.count(1);

        // Create a map of streams and starting entry IDs
        Map<String, StreamEntryID> streamEntries = new HashMap<>();
        streamEntries.put(this.streamId, StreamEntryID.UNRECEIVED_ENTRY);
        
        List<Map.Entry<String, List<StreamEntry>>> entries = null;
        try {
            // Fetch entries using xread method
            entries = jedis.xreadGroup("storm_group", "storm_consumer", xReadGroupParams, streamEntries);
        } catch (Exception e) {
            System.err.println(e);
        }

        if (entries == null) {
            return;
        }

        StreamEntry entry = entries.get(0).getValue().get(0); // read 1 tuple from 1 stream, hence getting 0th values
        String jsonFields = entry.getFields().get("data");
        JsonNode jsonNode;
        try {
            jsonNode = objectMapper.readTree(jsonFields);
            List<Object> values = new ArrayList<>();
            for (Field field : this.schemaConfig.getStreamById(this.streamId).getFields()) {
                if (field.getName().equals("tupleId")) {
                    values.add(entry.getID().toString());
                } else if (field.getType().equals("double")) {
                    values.add(jsonNode.get(field.getName()).asDouble());
                } else {
                    values.add(jsonNode.get(field.getName()).asText());
                }
            }

            values.add(this.streamId);
            collector.emit(new Values(values.toArray()), entry.getID().toString());
        } catch (IOException e) {
            throw new RuntimeException(e);
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
