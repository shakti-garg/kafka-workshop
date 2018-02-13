package com.wordcount_stream;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Properties;

import static org.apache.kafka.common.serialization.Serdes.StringSerde;
import static org.apache.kafka.streams.StreamsConfig.*;

public class WordCountStreamApp {

	private static final String BOOTSTRAP_SERVERS = "kafka-cluster:9092";

	public static void main(String[] args) {
		KafkaStreams streams = createStream();
		streams.start();
		Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
	}

	public static KafkaStreams createStream() {

		Properties props = new Properties();
		props.put(APPLICATION_ID_CONFIG, "stream-word-count");
		props.put(BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
		props.put(DEFAULT_KEY_SERDE_CLASS_CONFIG, StringSerde.class.getName());
		props.put(DEFAULT_VALUE_SERDE_CLASS_CONFIG, StringSerde.class.getName());
		props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
		props.put(StreamsConfig.STATE_DIR_CONFIG, "streams-store");

		StreamsBuilder builder = new StreamsBuilder();
		//Create Kstream from TEXT_LINE topic

		//Map Values to lowercase


		//FlatMapValues split by space (regex: "\\W+"))


		//Map to apply key


		//GroupByKey


		//Count occurrence in each group


		//Convert to stream (toStream)


		//Write result to output topic  WORD_COUNT_OUTPUT using to()

		return new KafkaStreams(builder.build(), props);
	}
}
