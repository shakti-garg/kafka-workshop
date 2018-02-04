import consumer.ConsumerClient;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.Assert;
import org.junit.Test;
import producer.ProducerClient;

import java.util.ArrayList;

import static org.hamcrest.CoreMatchers.is;

public class KafkaClientIntegrationTest {

	public static final int NUMBER_OF_RECORDS = 10;

	@Test
	public void shouldPublishAndConsumeMessages() {

		ProducerClient.publish(NUMBER_OF_RECORDS);

		ArrayList<ConsumerRecord<Integer, String>> consumedRecords = ConsumerClient.consumeTill(NUMBER_OF_RECORDS);

		Assert.assertThat(consumedRecords.size(), is(NUMBER_OF_RECORDS));

	}

}
