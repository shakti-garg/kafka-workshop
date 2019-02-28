package com.anomaly_detection_stream.serializer;

import com.anomaly_detection_stream.model.UserClick;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class UserClickSerde implements Serde<UserClick> {
  private UserClickSerializer serializer = new UserClickSerializer();
  private UserClickDeserializer deserializer = new UserClickDeserializer();

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    serializer.configure(configs, isKey);
    deserializer.configure(configs, isKey);
  }

  @Override
  public void close() {
    serializer.close();
    deserializer.close();
  }

  @Override
  public Serializer<UserClick> serializer() {
    return serializer;
  }

  @Override
  public Deserializer<UserClick> deserializer() {
    return deserializer;
  }
}