package serdes;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import model.UserClick;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class UserClickSerializer implements Serializer<UserClick> {

    private ObjectMapper objectMapper;

    @Override
    public void configure(Map<String, ?> map, boolean b) {
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public byte[] serialize(String topic, UserClick userClick) {
        try {
            return objectMapper.writeValueAsBytes(userClick);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return new byte[0];
    }

    @Override
    public void close() {

    }
}
