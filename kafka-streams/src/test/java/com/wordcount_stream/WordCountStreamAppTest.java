package com.wordcount_stream;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static com.wordcount_stream.TextLineProducer.createProducer;
import static com.wordcount_stream.WordCountStreamApp.createStream;

public class WordCountStreamAppTest {

	@Test
	public void shouldCountWord() throws Exception {
		KafkaStreams stream = createStream();
		stream.cleanUp();
		stream.start();

		String sentence1 = "Hello Kafka Stream World";
		String sentence2 = "Welcome to Kafka World";

		List<KeyValue<String, Long>> expectedWordCount = Arrays.asList(
				new KeyValue<>("Hello", 1L),
				new KeyValue<>("Kafka", 2L),
				new KeyValue<>("Stream", 1L),
				new KeyValue<>("World", 2L),
				new KeyValue<>("Welcome", 2L),
				new KeyValue<>("to", 2L)
		);

		TextLineProducer.publish(Arrays.asList(sentence1, sentence2));

		List<ConsumerRecord<String, Long>> consumerRecords =
				TextLineConsumer.consume(Topics.WORD_COUNT_OUTPUT, 10);

		for (ConsumerRecord consumerRecord : consumerRecords) {
			System.out.println(consumerRecord.key().toString());
			System.out.println(consumerRecord.value().toString());
		}

		Assert.assertThat(consumerRecords.size(), is(6));

		stream.close();

	}
}
