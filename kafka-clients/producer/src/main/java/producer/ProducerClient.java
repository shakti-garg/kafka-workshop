package producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class ProducerClient {
	public static void main(String[] args) {

		//Setup KafkaConfig properties

		Properties properties = new Properties();
		properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		properties.put(ProducerConfig.ACKS_CONFIG, "all");
		properties.put(ProducerConfig.RETRIES_CONFIG, 0);
		properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
		properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

		// Create Producer
		try (Producer<String, String> producer = new KafkaProducer<String, String>(properties)) {
			for (int i = 0; i < 100; i++) {
				producer.send(new ProducerRecord<String, String>("my-topic", Integer.toString(i), Integer.toString(i)));
			}
		}

		//Start Producer

	}
}