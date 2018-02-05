import consumer.AnomalousUserConsumer;
import model.UserClick;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.streams.KafkaStreams;
import org.junit.Assert;
import org.junit.Test;
import producer.UserClickEventProducer;
import streams.StreamApp;

import java.util.List;

import static org.hamcrest.CoreMatchers.is;

public class AnomalyDetectionTest {

	@Test
	public void shouldDetectAnomalousUser() throws Exception {

//		StreamApp.start();

		UserClickEventProducer.publishEvents();

		List<ConsumerRecord<String, UserClick>> consumerRecords = AnomalousUserConsumer.consume();

		Assert.assertNotNull(consumerRecords);
		Assert.assertThat(consumerRecords.size(), is(10));

//		Assert.assertThat(anomalousUsers.value().getUser(), is("Alice"));
	}
}
