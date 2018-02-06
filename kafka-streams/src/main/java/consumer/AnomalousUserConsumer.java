package consumer;

import model.UserClick;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import serdes.UserClickDeserializer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class AnomalousUserConsumer {
	private static final String BOOTSTRAP_SERVERS = "kafka-cluster:9092";
	private static final List<String> TOPICS = Arrays.asList("user-click");

	public static List<ConsumerRecord<String, UserClick>> consume() {
		Consumer<String, UserClick> consumer = createConsumer();
		int emptyResponse = 0;
		int limit = 10;

		List<ConsumerRecord<String, UserClick>> list = new ArrayList<>();

		while (emptyResponse < limit) {
			ConsumerRecords<String, UserClick> records = consumer.poll(1000);
			if(records.count() == 0) {
				emptyResponse += 1;
			}
			records.forEach(list::add);
		}
		consumer.commitSync();

		return list;
	}

	private static Consumer<String, UserClick> createConsumer() {
		final Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "KafkaExampleConsumer");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, UserClickDeserializer.class.getName());

		final Consumer<String, UserClick> consumer = new KafkaConsumer<>(props);

		consumer.subscribe(TOPICS);
		return consumer;
	}
}
