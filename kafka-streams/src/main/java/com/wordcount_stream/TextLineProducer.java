package com.wordcount_stream;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class TextLineProducer {

	private static final String BOOTSTRAP_SERVERS = "localhost:9092";

	public static void publish(List<String> sentences) {
		Producer<String, String> producer = createProducer();

		sentences.forEach(sentence -> {
			try {
				ProducerRecord<String, String> record = new ProducerRecord<>(Topics.TEXT_LINE, sentence);
				RecordMetadata metadata = producer.send(record).get();
				System.out.println(String.format("Message sent: key={%s}, value={%s}, offset={%d}, partition={%d}", record.key(),
						record.value(), metadata.offset(), metadata.partition()));
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (ExecutionException e) {
				e.printStackTrace();
			}
		});
	}

	public static Producer<String, String> createProducer() {
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
				BOOTSTRAP_SERVERS);
		props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaWordCountExampleProducer");
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
				StringSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
				StringSerializer.class.getName());
		return new KafkaProducer<>(props);
	}
}
