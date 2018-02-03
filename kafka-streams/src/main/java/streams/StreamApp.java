package streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.Processor;

import java.util.Arrays;
import java.util.Properties;

import static org.apache.kafka.streams.StreamsConfig.*;

public class StreamApp {

    private static final String BOOTSTRAP_SERVERS = "kafka-cluster:9092";
    private static final String TOPIC = "my-topic";

    public static void main(String[] args) {
        KafkaStreams streams = createStream();
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private static KafkaStreams createStream() {
        Properties props = new Properties();
        props.put(APPLICATION_ID_CONFIG, "stream-my-topic");
        props.put(BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.IntegerSerde.class.getName());
        props.put(DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

        StreamsBuilder builder = new StreamsBuilder();
        KStream<Integer, String> source = builder.stream(TOPIC);
        source.filter((key, value) -> key % 2 == 0).to("filtered-my-topic");

        return new KafkaStreams(builder.build(), props);
    }
}
