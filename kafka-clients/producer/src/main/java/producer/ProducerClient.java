package producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerClient {
    private static final Logger logger = LoggerFactory.getLogger(ProducerClient.class);

    private static final String TOPIC = "my-topic";
    private static final String BOOTSTRAP_SERVERS = "kafka-cluster:9092";

    public static void main(String[] args) {
        logger.info("started");
        final Producer<Integer, String> producer = createProducer();
        // Create Producer

        for (int i = 0; i < 100; i++) {
            ProducerRecord<Integer, String> record =
                    new ProducerRecord<>(TOPIC, i, "message count: " + i);

            try {
                RecordMetadata metadata = producer.send(record).get();
                logger.info("Message sent: record={}, metadata={}", record, metadata);
            } catch (Exception e) {
                logger.error(e.getMessage());
            }
        }

        producer.close();
    }

    private static Producer<Integer, String> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaExampleProducer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                IntegerSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        return new KafkaProducer<>(props);
    }
}