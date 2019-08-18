package com.max_value_per_key;

import org.apache.kafka.common.serialization.*;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.streams.test.OutputVerifier;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.*;

public class MaxValuePerKeyStreamAppTest {

  private TopologyTestDriver testDriver;
  private KeyValueStore<String, Long> store;

  private StringDeserializer stringDeserializer = new StringDeserializer();
  private LongDeserializer longDeserializer = new LongDeserializer();
  private ConsumerRecordFactory<String, Long> recordFactory = new ConsumerRecordFactory<>(new StringSerializer(), new LongSerializer());

  @Before
  public void setup() {
    Topology topology = MaxValuePerKeyStreamApp.createTopology();

    // setup test driver
    Properties props = new Properties();
    props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "maxAggregation");
    props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
    props.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    props.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Long().getClass().getName());

    testDriver = new TopologyTestDriver(topology, props);

    // pre-populate store
    store = testDriver.getKeyValueStore("aggStore");
    store.put("a", 21L);
  }

  @After
  public void tearDown() {
    testDriver.close();
  }

  @Test
  public void shouldFlushStoreForFirstInput() {
    testDriver.pipeInput(recordFactory.create("input-topic", "a", 1L, 9999L));

    OutputVerifier.compareKeyValue(testDriver.readOutput("result-topic", stringDeserializer, longDeserializer), "a", 21L);
    Assert.assertNull(testDriver.readOutput("result-topic", stringDeserializer, longDeserializer));
  }

  @Test
  public void shouldNotUpdateStoreForSmallerValue() {
    testDriver.pipeInput(recordFactory.create("input-topic", "a", 1L, 9999L));

    Assert.assertThat(store.get("a"), equalTo(21L));
    OutputVerifier.compareKeyValue(testDriver.readOutput("result-topic", stringDeserializer, longDeserializer), "a", 21L);
    Assert.assertNull(testDriver.readOutput("result-topic", stringDeserializer, longDeserializer));
  }

  @Test
  public void shouldUpdateStoreForLargerValue() {
    testDriver.pipeInput(recordFactory.create("input-topic", "a", 42L, 9999L));

    Assert.assertThat(store.get("a"), equalTo(42L));
    OutputVerifier.compareKeyValue(testDriver.readOutput("result-topic", stringDeserializer, longDeserializer), "a", 42L);
    Assert.assertNull(testDriver.readOutput("result-topic", stringDeserializer, longDeserializer));
  }

  @Test
  public void shouldPunctuateIfStreamTimeAdvances() {
    testDriver.pipeInput(recordFactory.create("input-topic", "a", 1L, 9999L));
    OutputVerifier.compareKeyValue(testDriver.readOutput("result-topic", stringDeserializer, longDeserializer), "a", 21L);

    testDriver.pipeInput(recordFactory.create("input-topic", "a", 42L, 9999L));
    Assert.assertNull(testDriver.readOutput("result-topic", stringDeserializer, longDeserializer));

    testDriver.pipeInput(recordFactory.create("input-topic", "a", 1L, 10000L));
    OutputVerifier.compareKeyValue(testDriver.readOutput("result-topic", stringDeserializer, longDeserializer), "a", 42L);
    Assert.assertNull(testDriver.readOutput("result-topic", stringDeserializer, longDeserializer));
  }

  @Test
  public void shouldPunctuateIfWallClockTimeAdvances() {
    testDriver.advanceWallClockTime(60000);
    OutputVerifier.compareKeyValue(testDriver.readOutput("result-topic", stringDeserializer, longDeserializer), "a", 21L);
    Assert.assertNull(testDriver.readOutput("result-topic", stringDeserializer, longDeserializer));
  }

  @Test
  public void shouldPunctuateIfBothStreamAndWallClockTimeAdvances() {
    testDriver.pipeInput(recordFactory.create("input-topic", "a", 1L, 9999L));
    OutputVerifier.compareKeyValue(testDriver.readOutput("result-topic", stringDeserializer, longDeserializer), "a", 21L);

    testDriver.pipeInput(recordFactory.create("input-topic", "a", 42L, 9999L));
    testDriver.advanceWallClockTime(60000);
    OutputVerifier.compareKeyValue(testDriver.readOutput("result-topic", stringDeserializer, longDeserializer), "a", 42L);

    testDriver.pipeInput(recordFactory.create("input-topic", "a", 84L, 10000L));
    OutputVerifier.compareKeyValue(testDriver.readOutput("result-topic", stringDeserializer, longDeserializer), "a", 84L);
    Assert.assertNull(testDriver.readOutput("result-topic", stringDeserializer, longDeserializer));
  }

}