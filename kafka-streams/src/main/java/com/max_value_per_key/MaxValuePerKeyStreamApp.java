package com.max_value_per_key;

import com.wordcount_stream.Topics;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Properties;

import static org.apache.kafka.common.serialization.Serdes.StringSerde;
import static org.apache.kafka.streams.StreamsConfig.*;

public class MaxValuePerKeyStreamApp {

	private static final Logger logger = LoggerFactory.getLogger(MaxValuePerKeyStreamApp.class);

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
		props.put(DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.LongSerde.class.getName());
		props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
		props.put(StreamsConfig.STATE_DIR_CONFIG, "streams-store");

		Topology topology = createTopology();

    return new KafkaStreams(topology, props);
	}

	public static Topology createTopology() {
		Topology topology = new Topology();

		topology.addSource("sourceProcessor", "input-topic");
		topology.addProcessor("aggregator", new CustomMaxAggregatorSupplier(), "sourceProcessor");
		topology.addStateStore(
				Stores.keyValueStoreBuilder(
						Stores.inMemoryKeyValueStore("aggStore"),
						Serdes.String(), Serdes.Long()).withLoggingDisabled(), // need to disable logging to allow store pre-populating
				"aggregator");
		topology.addSink("sinkProcessor", "result-topic", "aggregator");

		logger.info(topology.describe().toString());
		return topology;
	}
}
