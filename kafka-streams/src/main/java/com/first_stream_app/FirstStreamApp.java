package com.first_stream_app;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import static org.apache.kafka.streams.StreamsConfig.*;

public class FirstStreamApp {

	private static final Logger logger = LoggerFactory.getLogger(FirstStreamApp.class);

	private static final String BOOTSTRAP_SERVERS = "localhost:9092";

	public static void main(String[] args) {
		start();
	}

	public static void start() {
		KafkaStreams streams = createStream();
		streams.start();

		Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
	}


	public static KafkaStreams createStream() {
		Properties props = new Properties();
		props.put(APPLICATION_ID_CONFIG, "stream-my-topic");
		props.put(BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
		props.put(DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.IntegerSerde.class.getName());
		props.put(DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
		props.put(COMMIT_INTERVAL_MS_CONFIG, "1000");

		StreamsBuilder builder = new StreamsBuilder();
		KStream<Integer, String> source = builder.stream(Topics.MY_TOPIC);
		//source.print();
		source.peek((key, value) -> logger.info("filtered record: ({}, {})", key, value))
				.to(Topics.FILTERED_MY_TOPIC);

    Topology topology = builder.build();
    logger.info(topology.describe().toString());

    return new KafkaStreams(topology, props);
	}
}
