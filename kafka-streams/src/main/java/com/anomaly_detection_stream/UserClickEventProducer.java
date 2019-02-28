package com.anomaly_detection_stream;

import com.anomaly_detection_stream.model.UserClick;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import com.anomaly_detection_stream.serializer.UserClickSerializer;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class UserClickEventProducer {

	private static final String BOOTSTRAP_SERVERS = "localhost:9092";
	private static final String USER_CLICK_TOPIC = "user-click";

	public static void publishEvents(List<UserClick> userClicks) {
		Producer<String, UserClick> producer = createProducer();
		
		userClicks.forEach(userClick -> {
      try {
        ProducerRecord<String, UserClick> record = new ProducerRecord<>(USER_CLICK_TOPIC, userClick);

        RecordMetadata metadata = producer.send(record).get();
        System.out.println(String.format("Message sent: key={%s}, value={%s}, offset={%d}, partition={%d}", record.key(),
          record.value(), metadata.offset(), metadata.partition()));
      } catch (InterruptedException e) {
        e.printStackTrace();
      } catch (ExecutionException e) {
        e.printStackTrace();
      }

      try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		});
	}

	private static Producer<String, UserClick> createProducer() {
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
		props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaExampleProducer");
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
				StringSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
				UserClickSerializer.class.getName());
		return new KafkaProducer<>(props);
	}
}
