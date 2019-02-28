package com.anomaly_detection_stream;

import com.anomaly_detection_stream.model.UserClick;
import com.anomaly_detection_stream.serializer.UserClickDeserializer;
import com.anomaly_detection_stream.serializer.UserClickSerde;
import com.anomaly_detection_stream.serializer.UserClickSerializer;
import com.wordcount_stream.WordCountStreamApp;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.String;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.common.serialization.Serdes.String;
import static org.apache.kafka.common.serialization.Serdes.*;
import static org.apache.kafka.streams.StreamsConfig.*;

public class AnomalyDetectionStreamApp {

	private static final Logger logger = LoggerFactory.getLogger(AnomalyDetectionStreamApp.class);

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
		props.put(APPLICATION_ID_CONFIG, "anomaly-detection-stream");
		props.put(BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
    props.put(DEFAULT_KEY_SERDE_CLASS_CONFIG, StringSerde.class.getName());
    props.put(DEFAULT_VALUE_SERDE_CLASS_CONFIG, UserClickSerde.class.getName());
    props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);

		StreamsBuilder builder = new StreamsBuilder();
		KStream<String, UserClick> stream = builder.stream(Topics.USER_CLICK_TOPIC);

		KGroupedStream<String, UserClick> groupedByUserIp = stream.groupBy(((key, value) -> {
      return new String(value.getUser().toUpperCase() + ":" + value.getIpAddress());
		}));

		KTable<String, Long> count = groupedByUserIp.count();

    KStream<String, Long> outputStream = count.toStream();

    outputStream.to(Topics.CLICK_COUNT, Produced.with(Serdes.String(), Serdes.Long()));

    Topology topology = builder.build();
		logger.info(topology.describe().toString());

		return new KafkaStreams(topology, props);
	}
}
