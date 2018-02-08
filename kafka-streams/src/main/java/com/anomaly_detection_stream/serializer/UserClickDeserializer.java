package com.anomaly_detection_stream.serializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.anomaly_detection_stream.model.UserClick;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;


public class UserClickDeserializer implements Deserializer<UserClick> {

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {

	}

	@Override
	public UserClick deserialize(String topic, byte[] data) {
		ObjectMapper mapper = new ObjectMapper();
		UserClick userClick = null;
		try {
			userClick = mapper.readValue(data, UserClick.class);
		} catch (Exception e) {

			e.printStackTrace();
		}
		return userClick;
	}

	@Override
	public void close() {

	}
}
