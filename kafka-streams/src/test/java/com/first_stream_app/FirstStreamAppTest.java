package com.first_stream_app;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.KafkaStreams;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;

import static org.hamcrest.CoreMatchers.is;

public class FirstStreamAppTest {

	public static final int NUMBER_OF_RECORDS = 10;

	@Test
	public void shouldFilterMyTopic() throws Exception {
		ProducerClient.publish(NUMBER_OF_RECORDS);
		KafkaStreams stream = FirstStreamApp.createStream();
		stream.start();

		ArrayList<ConsumerRecord<Integer, String>> consumerRecords = ConsumerClient.consumeFor(5);

		Assert.assertThat(consumerRecords.size(), is(5));

		Runtime.getRuntime().addShutdownHook(new Thread(stream::close));

	}
}