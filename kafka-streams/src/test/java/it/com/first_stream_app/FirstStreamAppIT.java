package com.first_stream_app;

import com.first_stream_app.ConsumerClient;
import com.first_stream_app.FirstStreamApp;
import com.first_stream_app.ProducerClient;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.KafkaStreams;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;

import static org.hamcrest.CoreMatchers.is;

public class FirstStreamAppIT {

	public static final int NUMBER_OF_RECORDS = 10;

	@Test
	public void shouldFilterMyTopic() throws Exception {
		ProducerClient.publish(NUMBER_OF_RECORDS);

		FirstStreamApp.start();

		ArrayList<ConsumerRecord<Integer, String>> consumerRecords = ConsumerClient.consumeFor(5);
		Assert.assertThat(consumerRecords.size(), is(10));

		//Runtime.getRuntime().addShutdownHook(new Thread(stream::close));

	}
}