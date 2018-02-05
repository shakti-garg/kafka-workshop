import consumer.AnomalousUserConsumer;
import model.UserClick;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.Assert;
import org.junit.Test;
import producer.UserClickEventProducer;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;

public class AnomalyDetectionTest {

    @Test
    public void shouldDetectAnomalousUser() throws Exception {

        List<UserClick> userClicks = Arrays.asList(
                new UserClick("bob", "10.1.2.5"),
                new UserClick("alice", "192.168.4.2"),
                new UserClick("John", "10.11.2.1"),
                new UserClick("alice", "192.168.4.2"),
                new UserClick("bob", "10.1.2.5"),
                new UserClick("alice", "192.168.4.2"),
                new UserClick("bob", "10.1.2.5"),
                new UserClick("alice", "192.168.4.2"),
                new UserClick("alice", "192.168.4.2"),
                new UserClick("bob", "10.1.2.5")
        );

        UserClickEventProducer.publishEvents(userClicks);

        List<ConsumerRecord<String, UserClick>> consumerRecords = AnomalousUserConsumer.consume();

        Assert.assertNotNull(consumerRecords);
        Assert.assertThat(consumerRecords.size(), is(1));
        Assert.assertThat(consumerRecords.get(0).value().getUser(), is("alice"));
    }
}
