package consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class ConsumerClient {

	private static final Logger logger = LoggerFactory.getLogger(ConsumerClient.class);

	private static final String BOOTSTRAP_SERVERS = "kafka-cluster:9092";
	private static final List<String> TOPICS = Arrays.asList("my-topic", "filtered-my-topic");

	public static void main(String[] args) {
		logger.info("here");
		Consumer<Integer, String> consumer = createConsumer();
		int limit = 100;
		int emptyResponse = 0;
		while (true) {
			final ConsumerRecords<Integer, String> records = consumer.poll(1000);

			if (records.isEmpty()) {
				emptyResponse += 1;
				if (emptyResponse > limit)
					break;
			}

			records.forEach(record -> logger.info("Message received: topic={}, key={}, value={}, offset={}, partition={}",
					record.topic(), record.key(), record.value(), record.offset()));

			consumer.commitSync();
		}
		consumer.close();

	}

	private static Consumer<Integer, String> createConsumer() {
		final Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "KafkaExampleConsumer");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

		final Consumer<Integer, String> consumer = new KafkaConsumer<>(props);

		consumer.subscribe(TOPICS);
		return consumer;
	}
}