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

    public RedisStreamSpout() {
        this.schemaConfig = SchemaConfigBuilder.build();
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        this.objectMapper = new ObjectMapper();

        this.jedis = new Jedis("redis", 6379); // Set your Redis server details

        try {
            for (Stream stream : this.schemaConfig.getStreams()) {
                jedis.xgroupCreate(stream.getId(), "storm_group", StreamEntryID.LAST_ENTRY, false);
                jedis.xgroupCreateConsumer(stream.getId(), "storm_group", "storm_consumer");
            }
        } catch (Exception e) {
            // Group already exists
        }


    }

    @Override
    public void nextTuple() {
        // Fetch data from Redis stream for all unreceived entries
        XReadGroupParams xReadGroupParams = new XReadGroupParams();
//        xReadGroupParams.block(5000);
        xReadGroupParams.count(1000);

        // Create a map of streams and starting entry IDs
        Map<String, StreamEntryID> streamEntries = new HashMap<>();
        for (Stream stream : this.schemaConfig.getStreams()) {
            streamEntries.put(stream.getId(), StreamEntryID.UNRECEIVED_ENTRY);
        }

        // Fetch entries using xread method
        List<Map.Entry<String, List<StreamEntry>>> entries = jedis.xreadGroup("storm_group", "storm_consumer", xReadGroupParams, streamEntries);

        if (entries == null) {
            return;
        }

        int i = 0;
        for (Stream stream : this.schemaConfig.getStreams()) {
            if (i >= entries.size()) {
                break;
            }
            String streamId = entries.get(i).getKey();
            List<StreamEntry> entriesForStream = entries.get(i).getValue();
            for (StreamEntry entry : entriesForStream) {
                String jsonFields = entry.getFields().get("data");
                JsonNode jsonNode;
                try {
                    jsonNode = objectMapper.readTree(jsonFields);
                    List<Object> values = new ArrayList<>();
                    for (Field field : stream.getFields()) {
                        if (field.getName().equals("tupleId")) {
                            values.add(entry.getID().toString());
                        } else if (field.getType().equals("double")) {
                            values.add(jsonNode.get(field.getName()).asDouble());
                        } else {
                            values.add(jsonNode.get(field.getName()).asText());
                        }
                    }
                    collector.emit(streamId, new Values(values.toArray()));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            i += 1;
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // Declare the output fields
        declarer.declare(new Fields("entryId", "jsonFields"));
        for (Stream stream : this.schemaConfig.getStreams()) {
            declarer.declareStream(stream.getId(), new Fields(stream.getFieldNames()));
        }
    }

    @Override
    public void ack(Object msgId) {
        // Acknowledgment logic (if needed)
    }

    @Override
    public void fail(Object msgId) {
        // Failure handling logic (if needed)
    }

    @Override
    public void close() {
        // Close resources (if needed)
        jedis.close();
    }
}
