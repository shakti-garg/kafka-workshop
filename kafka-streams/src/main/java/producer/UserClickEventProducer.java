package producer;

import model.UserClick;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.json.JsonSerializer;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class UserClickEventProducer {


	private static final String BOOTSTRAP_SERVERS = "localhost:9092";
	private static final List<String> TOPICS = Arrays.asList("my-topic", "filtered-my-topic");
	public static void publishEvents() {
		Producer<String, UserClick> producer = createProducer();
	}

	private static Producer<String, UserClick> createProducer() {
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
				BOOTSTRAP_SERVERS);
		props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaExampleProducer");
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
				StringSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
				JsonSerializer.class.getName());
		return new KafkaProducer<>(props);
	}
}
