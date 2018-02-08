package utils;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.streams.KeyValue;

import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

public class IntegrationTestUtil {

	public static<K,V> void produceValues(String inputTopic, Collection<KeyValue<K,V>> records, Properties producerConfig){

		List<KeyValue<Object, KeyValue<K, V>>> keyedRecords =
				records.stream().map(record -> new KeyValue<>(null, record)).collect(Collectors.toList());

		try (KafkaProducer<K, V> kafkaProducer = new KafkaProducer<>(producerConfig)) {
			for (KeyValue<Object, KeyValue<K, V>> record : keyedRecords){

			}
		}
	}


}
