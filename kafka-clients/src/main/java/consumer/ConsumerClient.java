package consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class ConsumerClient {

	private static final Logger logger = LoggerFactory.getLogger(ConsumerClient.class);

	private static final String BOOTSTRAP_SERVERS = "localhost:9092";
	private static final List<String> TOPICS = Arrays.asList("my-topic");

	public static ArrayList<ConsumerRecord<Integer, String>> consumeTill(int limit) {
		logger.info("here");

		Consumer<Integer, String> consumer = createConsumer();

		ArrayList<ConsumerRecord<Integer, String>> consumedRecords = new ArrayList<>();

		while(consumedRecords.size() < limit) {

			final ConsumerRecords<Integer, String> records = consumer.poll(0);

			records.forEach(
					record -> {
						logger.info("Message received: topic={}, key={}, value={}, offset={}, partition={}",
								record.topic(), record.key(), record.value(), record.offset(), record.partition());
						consumedRecords.add(record);
					});
			consumer.commitSync();
		}

		consumer.close();
		return consumedRecords;
	}


	private static Consumer<Integer, String> createConsumer() {

		final Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "KafkaExampleConsumer2");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

		final Consumer<Integer, String> consumer = new KafkaConsumer<>(props);

		consumer.subscribe(TOPICS);
		return consumer;
	}
}