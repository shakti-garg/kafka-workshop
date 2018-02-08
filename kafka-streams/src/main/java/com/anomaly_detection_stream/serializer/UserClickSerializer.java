package com.anomaly_detection_stream.serializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.anomaly_detection_stream.model.UserClick;
import org.apache.kafka.common.serialization.Serializer;

import java.io.IOException;
import java.util.Map;


public class UserClickSerializer implements Serializer<UserClick> {
	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {

	}

	@Override
	public byte[] serialize(String topic, UserClick data) {
		byte[] retValue = new byte[0];
		ObjectMapper objectMapper = new ObjectMapper();
		try {
			retValue = objectMapper.writeValueAsString(data).getBytes();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return retValue;
	}

	@Override
	public void close() {

	}
}
