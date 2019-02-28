package com.first_stream_app;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class ConsumerClient {

	private static final Logger logger = LoggerFactory.getLogger(ConsumerClient.class);

	private static final String BOOTSTRAP_SERVERS = "localhost:9092";
	private static final List<String> TOPICS = Arrays.asList(Topics.FILTERED_MY_TOPIC);

	public static ArrayList<ConsumerRecord<Integer, String>> consumeFor(int timeoutInSec) {

		logger.info("here");
		Consumer<Integer, String> consumer = createConsumer();

		long startTime = System.currentTimeMillis();

		ArrayList<ConsumerRecord<Integer, String>> consumedRecords = new ArrayList<>();

		while ((System.currentTimeMillis() - startTime) < (timeoutInSec * 1000)) {
			final ConsumerRecords<Integer, String> records = consumer.poll(100);

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
		props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "KafkaExampleConsumer");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

		final Consumer<Integer, String> consumer = new KafkaConsumer<>(props);

		consumer.subscribe(TOPICS);
		return consumer;
	}
}