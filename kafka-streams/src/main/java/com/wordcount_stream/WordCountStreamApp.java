package com.wordcount_stream;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.WindowStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import static org.apache.kafka.streams.StreamsConfig.*;
import static org.apache.kafka.streams.kstream.Suppressed.BufferConfig.unbounded;

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
		props.put(DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
    props.put(DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
    props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
    props.put(StreamsConfig.STATE_DIR_CONFIG, "streams-store");

    StreamsBuilder builder = new StreamsBuilder();

    Topology topology = createTopology(builder, 1000).build();
    logger.info(topology.describe().toString());

    return new KafkaStreams(topology, props);
	}

    public static StreamsBuilder createTopologyWithAggregate(StreamsBuilder builder) {
        KGroupedStream<String, String> groupedStream = getGroupedWordStream(builder);

        //Count occurrence in each group
        KTable<String, Long> wordCount = groupedStream
                .aggregate(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("word_count_state_store"));

        //Convert to stream (toStream)
        KStream<String, Long> outputStream = wordCount.toStream();

        //Write result to output topic  WORD_COUNT_OUTPUT using to()
        outputStream.to(Topics.WORD_COUNT_OUTPUT, Produced.with(Serdes.String(), Serdes.Long()));

        return builder;
    }

	public static StreamsBuilder createTopology(StreamsBuilder builder) {
    KGroupedStream<String, String> groupedStream = getGroupedWordStream(builder);

		//Count occurrence in each group
		KTable<String, Long> wordCount = groupedStream
        .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("word_count_state_store"));

		//Convert to stream (toStream)
		KStream<String, Long> outputStream = wordCount.toStream();

		//Write result to output topic  WORD_COUNT_OUTPUT using to()
    outputStream.to(Topics.WORD_COUNT_OUTPUT, Produced.with(Serdes.String(), Serdes.Long()));

    return builder;
	}

  public static StreamsBuilder createTopology(StreamsBuilder builder, long suppressOutputInMS) {
    KGroupedStream<String, String> groupedStream = getGroupedWordStream(builder);

    //Count occurrence in each group
    KTable<String, Long> wordCount = groupedStream
        .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("word_count_state_store"));

    if(suppressOutputInMS > 0){
      wordCount = wordCount.suppress(Suppressed.untilTimeLimit(Duration.ofMillis(suppressOutputInMS)
          , unbounded()));
    }

    //Convert to stream (toStream)
    KStream<String, Long> outputStream = wordCount.toStream();

    //Write result to output topic  WORD_COUNT_OUTPUT using to()
    outputStream.to(Topics.WORD_COUNT_OUTPUT, Produced.with(Serdes.String(), Serdes.Long()));

    return builder;
  }

  public static StreamsBuilder createWindowedTopology(StreamsBuilder builder, long windowDurationInMS) {
    KGroupedStream<String, String> groupedStream = getGroupedWordStream(builder);

    //Count occurrence in each group
    KTable<Windowed<String>, Long> wordCount = groupedStream.windowedBy(TimeWindows.of(Duration.ofMillis(windowDurationInMS)))
        .count(Materialized.<String, Long, WindowStore<Bytes, byte[]>>as("word_count_window_state_store"));
        //.suppress(Suppressed.untilWindowCloses(unbounded()));

    //Convert to stream (toStream)
    KStream<String, Long> outputStream = wordCount.toStream(
        ((key, value) -> String.format("%s@%d/%d", key.key(), key.window().start(), key.window().end())));

    //Write result to output topic  WORD_COUNT_OUTPUT using to()
    outputStream.to(Topics.WORD_COUNT_OUTPUT, Produced.with(Serdes.String(), Serdes.Long()));

    return builder;
  }

  private static KGroupedStream<String, String> getGroupedWordStream(StreamsBuilder builder){
    //Create Kstream from TEXT_LINE topic
    KStream<String, String> stream = builder.stream(Topics.TEXT_LINE, Consumed.with(Serdes.String(), Serdes.String()));

    //Map Values to lowercase
    KStream<String, String> upperStream = stream.mapValues(value -> value.toLowerCase());

    //FlatMapValues split by space (regex: "\\W+"))
    KStream<String, String> wordStream = upperStream.flatMapValues(value -> Arrays.asList(value.split("\\W+")));

    //GroupByKey
    KGroupedStream<String, String> groupedStream = wordStream.groupBy((key, value) -> value
        , Grouped.with(Serdes.String(), Serdes.String()));

    return groupedStream;
  }


}
