package com.wordcount_stream;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import java.lang.String;
import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;

import static org.apache.kafka.common.serialization.Serdes.String;
import static org.apache.kafka.common.serialization.Serdes.*;
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
		props.put(StreamsConfig.STATE_DIR_CONFIG, "streams-store");

		StreamsBuilder builder = new StreamsBuilder();
		KStream<String, String> sentences = builder.stream(Topics.SENTENCES, Consumed.with(String(), String()));
		KStream<String, Long> count = sentences.
				flatMapValues(value -> Arrays.asList(value.toLowerCase(Locale.getDefault()).split("\\W+")))
				.groupBy((key, value) -> value)
				.count()
				.toStream();

		count.to(Topics.WORD_COUNT_OUTPUT, Produced.with(Serdes.String(), Serdes.Long()));

		return new KafkaStreams(builder.build(), props);
	}
}
