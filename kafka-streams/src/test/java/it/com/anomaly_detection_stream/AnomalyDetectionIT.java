package com.anomaly_detection_stream;

import com.anomaly_detection_stream.AnomalyDetectionStreamApp;
import com.anomaly_detection_stream.GenericConsumer;
import com.anomaly_detection_stream.Topics;
import com.anomaly_detection_stream.UserClickEventProducer;
import com.anomaly_detection_stream.model.UserClick;
import com.anomaly_detection_stream.serializer.UserClickDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.KafkaStreams;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

public class AnomalyDetectionIT {
	private static final String BOOTSTRAP_SERVERS = "localhost:9092";

	@Test
	public void shouldDetectAnomalousUser() throws Exception {
		AnomalyDetectionStreamApp.main(new String[]{});

		List<UserClick> userClicks = Arrays.asList(
				new UserClick("bob", "10.1.2.5"),
				new UserClick("alice", "192.168.4.2"),
				new UserClick("john", "10.11.2.1"),
				new UserClick("alice", "192.168.4.2"),
				new UserClick("bob", "10.1.2.5"),
				new UserClick("alice", "192.168.4.2"),
				new UserClick("bob", "10.1.2.5"),
				new UserClick("alice", "192.168.4.2"),
				new UserClick("alice", "192.168.4.2"),
				new UserClick("bob", "10.1.2.5")
		);

		UserClickEventProducer.publishEvents(userClicks);

		final Properties useClickConsumerConfig = new Properties();
		useClickConsumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
		useClickConsumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "KafkaExampleConsumer");
		useClickConsumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		useClickConsumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, UserClickDeserializer.class.getName());

		List<ConsumerRecord<String, UserClick>> useClickRecords = GenericConsumer.consumeRecords(Topics.USER_CLICK_TOPIC, useClickConsumerConfig, 10);
		print(useClickRecords);

		assertNotNull(useClickRecords);
		assertThat(useClickRecords.size(), is(10));

		final Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "KafkaAnomalyDetectionExampleConsumer");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());

		List<ConsumerRecord<String, Long>> numberOfClicksPerUser = GenericConsumer.consumeRecords(Topics.CLICK_COUNT, props, 3);
		print(numberOfClicksPerUser);

		assertNotNull(numberOfClicksPerUser);
		assertThat(numberOfClicksPerUser.size(), is(3));
	}

	private <K, V> void print(List<ConsumerRecord<K, V>> records) {
		System.out.println("***************************");
		for (ConsumerRecord<K, V> record : records) {
			System.out.println(record.key());
			System.out.println(record.value());
		}
		System.out.println("***************************");
	}
}
