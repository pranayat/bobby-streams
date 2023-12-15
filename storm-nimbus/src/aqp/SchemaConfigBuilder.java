package aqp;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.InputStream;

public abstract class SchemaConfigBuilder {
    public static SchemaConfig build() {

        String schemaConfigString;
        SchemaConfig schemaConfig;
        try (InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream("schema.json")) {
            ObjectMapper mapper = new ObjectMapper();
            schemaConfigString = mapper.writeValueAsString(mapper.readValue(in, JsonNode.class));
            schemaConfig = mapper.readValue(schemaConfigString, SchemaConfig.class);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return schemaConfig;
    }


}
