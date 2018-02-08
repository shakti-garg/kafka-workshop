package com.anomaly_detection_stream;

import com.anomaly_detection_stream.model.UserClick;
import com.anomaly_detection_stream.serializer.UserClickDeserializer;
import com.anomaly_detection_stream.serializer.UserClickSerializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;

import java.lang.String;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.common.serialization.Serdes.String;
import static org.apache.kafka.common.serialization.Serdes.*;
import static org.apache.kafka.streams.StreamsConfig.*;

public class AnomalyDetectionStreamApp {


	private static final String BOOTSTRAP_SERVERS = "kafka-cluster:9092";

	public static void main(String[] args) {
		start();
	}

	public static void start() {
		KafkaStreams streams = createStream();
		streams.start();

		Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
	}


	public static KafkaStreams createStream() {
		Serde<UserClick> userClickSerdes = serdeFrom(new UserClickSerializer(), new UserClickDeserializer());

		Properties props = new Properties();
		props.put(APPLICATION_ID_CONFIG, "anomaly-detection-stream");
		props.put(BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
		props.put(DEFAULT_KEY_SERDE_CLASS_CONFIG, StringSerde.class.getName());
		props.put(DEFAULT_VALUE_SERDE_CLASS_CONFIG, StringSerde.class.getName());
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

		StreamsBuilder builder = new StreamsBuilder();

		KStream<String, UserClick> source = builder.stream(Topics.USER_CLICK_TOPIC, Consumed.with(String(), userClickSerdes));

		KGroupedStream<String, UserClick> stringUserClickKGroupedStream = source.map(((key, value) -> new KeyValue<>(value.getUser(), value)))
				.groupByKey(Serialized.with(String(), userClickSerdes));

		KTable<Windowed<String>, Long> count = stringUserClickKGroupedStream.windowedBy(
				TimeWindows.of(TimeUnit.MINUTES.toMillis(1)))
				.count();

		KTable<Windowed<String>, Long> filterUser = count.filter((key, value) -> value >= 5);

		KStream<Windowed<String>, Long> windowedLongKStream = filterUser.toStream().filter((key, value) -> value != null);

		KStream<String, Long> map = windowedLongKStream.map((key, value) -> new KeyValue<>(key.key(), value));
		map.to(Topics.ANOMALOUS_USER_TOPIC, Produced.with(Serdes.String(), Serdes.Long()));

		return new KafkaStreams(builder.build(), props);
	}
}
