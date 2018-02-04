import consumer.AnomalousUserConsumer;
import model.UserClick;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.Assert;
import org.junit.Test;
import producer.UserClickEventProducer;
import streams.StreamApp;

import static org.hamcrest.CoreMatchers.is;

public class AnomalyDetectionTest {
	@Test
	public void shouldDetectAnomalousUser() throws Exception {
		UserClickEventProducer.publishEvents();

		StreamApp.start();

		ConsumerRecord<String, UserClick> anomalousUser = AnomalousUserConsumer.consume();

		Assert.assertThat(anomalousUser.value().getUser(), is("Alice"));
	}
}
