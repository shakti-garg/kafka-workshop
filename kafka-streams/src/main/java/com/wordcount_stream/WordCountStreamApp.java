package com.wordcount_stream;

import com.first_stream_app.FirstStreamApp;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Properties;

import static org.apache.kafka.common.serialization.Serdes.StringSerde;
import static org.apache.kafka.streams.StreamsConfig.*;

public class WordCountStreamApp {

  private static final Logger logger = LoggerFactory.getLogger(WordCountStreamApp.class);

  private static final String BOOTSTRAP_SERVERS = "localhost:9092";

	public static void main(String[] args) {
		KafkaStreams streams = createStream();
		streams.cleanUp();
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
		KStream<String, String> stream = builder.stream(Topics.TEXT_LINE);

		//Map Values to lowercase
    KStream<String, String> upperStream = stream.mapValues(value -> value.toLowerCase());

    //FlatMapValues split by space (regex: "\\W+"))
    KStream<String, String> wordStream = upperStream.flatMapValues(value -> Arrays.asList(value.split("\\W+")));

    //GroupByKey
    KGroupedStream<String, String> groupedStream = wordStream.groupBy((key, value) -> value);

    //Count occurrence in each group
    KTable<String, Long> wordCount = groupedStream.count();

    //Convert to stream (toStream)
    KStream<String, Long> outputStream = wordCount.toStream();

    //Write result to output topic  WORD_COUNT_OUTPUT using to()
    outputStream.to(Topics.WORD_COUNT_OUTPUT, Produced.with(Serdes.String(), Serdes.Long()));

    Topology topology = builder.build();
    logger.info(topology.describe().toString());

    return new KafkaStreams(topology, props);
	}
}
