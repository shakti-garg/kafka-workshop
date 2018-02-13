package com.anomaly_detection_stream;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class GenericConsumer {
	public static final long DEFAULT_TIMEOUT = 30 * 1000L;

	public static <K, V> List<ConsumerRecord<K, V>> consumeRecords(String topic, Properties consumerConfig, int expectedNumOfRecords) throws InterruptedException {

		Consumer<K, V> consumer = createConsumer(topic, consumerConfig);

		List<ConsumerRecord<K, V>> accumData = new ArrayList<>();
		long startTime = System.currentTimeMillis();

		while (true) {

			ConsumerRecords<K, V> records = consumer.poll(1000);

			records.forEach(accumData::add);
			if (accumData.size() >= expectedNumOfRecords) {
				consumer.close();
				return accumData;
			}

			if (System.currentTimeMillis() > startTime + DEFAULT_TIMEOUT) {
				consumer.close();
				throw new AssertionError("Expected " + expectedNumOfRecords +
						" but received only " + accumData.size() +
						" records before timeout " + DEFAULT_TIMEOUT + " ms");
			}
		}

	}

	private static <K, V> Consumer<K, V> createConsumer(String topic, Properties consumerConfig) {
		final Consumer<K, V> consumer = new KafkaConsumer<>(consumerConfig);
		List<String> topics = Arrays.asList(topic);
		consumer.subscribe(topics);
		return consumer;
	}
}
