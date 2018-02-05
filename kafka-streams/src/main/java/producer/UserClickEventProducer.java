package producer;

import model.UserClick;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import serializer.UserClickSerializer;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class UserClickEventProducer {

	private static final String BOOTSTRAP_SERVERS = "kafka-cluster:9092";
	private static final List<String> TOPICS = Arrays.asList("my-topic", "filtered-my-topic");

	private static final String USER_CLICK_TOPIC = "user-click";

	private static final List<UserClick> userClicks = Arrays.asList(
			new UserClick("bob", "10.1.2.5"),
			new UserClick("alice", "192.168.4.2"),
			new UserClick("John", "10.11.2.1"),
			new UserClick("alice", "192.168.4.2"),
			new UserClick("bob", "10.1.2.5"),
			new UserClick("alice", "192.168.4.2"),
			new UserClick("bob", "10.1.2.5"),
			new UserClick("alice", "192.168.4.2"),
			new UserClick("alice", "192.168.4.2"),
			new UserClick("bob", "10.1.2.5")
	);

	public static void publishEvents() {

		Producer<String, UserClick> producer = createProducer();
		userClicks.forEach(userClick -> {

			ProducerRecord<String, UserClick> record = new ProducerRecord<>(USER_CLICK_TOPIC, userClick);
			producer.send(record);

			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		});
	}

	private static Producer<String, UserClick> createProducer() {
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
				BOOTSTRAP_SERVERS);
		props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaExampleProducer");
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
				StringSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
				UserClickSerializer.class.getName());
		return new KafkaProducer<>(props);
	}
}