package serdes;

import com.fasterxml.jackson.databind.ObjectMapper;
import model.UserClick;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.util.Map;

public class UserClickDeserializer implements Deserializer<UserClick> {

    private UserClick userClick;
    private ObjectMapper objectMapper;

    public UserClickDeserializer(UserClick userClick) {
        this.userClick = userClick;
        this.objectMapper = new ObjectMapper();
    }

    public UserClickDeserializer() {
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public void configure(Map<String, ?> map, boolean b) {
        if(userClick == null) {
            userClick = (UserClick) map.get("userClick");
        }
    }

    @Override
    public UserClick deserialize(String s, byte[] bytes) {
         if(bytes == null){
             return null;
         }
        try {
            return objectMapper.readValue(new String(bytes), UserClick.class);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public void close() {

    }
}
