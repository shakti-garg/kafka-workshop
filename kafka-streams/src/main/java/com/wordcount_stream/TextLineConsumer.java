package com.wordcount_stream;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class TextLineConsumer {
	private static final String BOOTSTRAP_SERVERS = "kafka-cluster:9092";


	public static List<ConsumerRecord<String, Long>> consume(String topic, int timeoutInSec) {
		Consumer<String, Long> consumer = createConsumer(topic);
		int responseCount = 0;
		ArrayList<ConsumerRecord<String, Long>> consumedRecords = new ArrayList<>();

		long startTime = System.currentTimeMillis();

		while ((System.currentTimeMillis() - startTime) < (timeoutInSec * 1000)) {

			final ConsumerRecords<String, Long> records = consumer.poll(1000);

			responseCount += records.count();

			records.forEach(
					record -> {
						consumedRecords.add(record);
					});
			consumer.commitSync();
		}
		consumer.close();
		return consumedRecords;
	}

	private static Consumer<String, Long> createConsumer(String topic) {
		final Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "KafkaWordCountExampleConsumer");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
		props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 1000);

		final Consumer<String, Long> consumer = new KafkaConsumer<>(props);
		List<String> topics = Arrays.asList(topic);
		consumer.subscribe(topics);
		return consumer;
	}
}
