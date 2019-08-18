package com.wordcount_stream;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.hamcrest.CoreMatchers.is;

public class WordCountStreamAppTest {

  public static final Logger logger = LoggerFactory.getLogger(WordCountStreamAppTest.class);

  private TopologyTestDriver testDriver;
  private Properties props;
  private StreamsBuilder builder;

  private static final Map<String, Long> expectedWordCountMap = new HashMap<String, Long> (){{
    put("hello", 1L);
    put("kafka", 2L);
    put("stream", 1L);
    put("world", 2L);
    put("welcome", 1L);
    put("to", 1L);
  }};


  @Before
  public void setUp(){
    props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-wordcount");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");

    builder = new StreamsBuilder();
  }

  @After
  public void tearDown() {
    testDriver.close();
  }

  @Test
  public void shouldCountWord() {
    testDriver = new TopologyTestDriver(WordCountStreamApp.createTopology(builder, 0).build(), props);

    //reset state store
    StateStore wordCountStateStore = testDriver.getStateStore("word_count_state_store");
    logger.info("state store:{} is persistent:{}", wordCountStateStore.name(), wordCountStateStore.persistent());

    //input
    ConsumerRecordFactory<String, String> recordFactory = new ConsumerRecordFactory<>(Topics.TEXT_LINE, new StringSerializer(), new StringSerializer());
    testDriver.pipeInput(recordFactory.create("Hello Kafka Stream World"));
    testDriver.pipeInput(recordFactory.create("Welcome to Kafka World"));

    //output
    List<KeyValue<String, Long>> outputWordCount = new ArrayList<>();
    Map<String, Long> outputWordCountMap = new HashMap<>();

    ProducerRecord<String, Long> outputRecord;
    while (null != (outputRecord = testDriver.readOutput(Topics.WORD_COUNT_OUTPUT, new StringDeserializer(), new LongDeserializer()))) {
      logger.info("Key: {}, Value: {}", outputRecord.key(), outputRecord.value());

      outputWordCount.add(new KeyValue<>(outputRecord.key(), outputRecord.value()));
      outputWordCountMap.put(outputRecord.key(), outputRecord.value());
    }

    //verify
    List<KeyValue<String, Long>> expectedWordCount = Arrays.asList(
        new KeyValue<>("hello", 1L),
        new KeyValue<>("kafka", 1L),
        new KeyValue<>("stream", 1L),
        new KeyValue<>("world", 1L),
        new KeyValue<>("welcome", 1L),
        new KeyValue<>("to", 1L),
        new KeyValue<>("kafka", 2L),
        new KeyValue<>("world", 2L)
    );

    Assert.assertEquals(expectedWordCount, outputWordCount);
    Assert.assertEquals(expectedWordCountMap, outputWordCountMap);
  }

  @Test
  public void shouldCountWordAndSuppressOutputBy30Secs() {
    testDriver = new TopologyTestDriver(WordCountStreamApp.createTopology(builder, 30000).build(), props);

    //reset state store
    StateStore wordCountStateStore = testDriver.getStateStore("word_count_state_store");
    logger.info("state store:{} is persistent:{}", wordCountStateStore.name(), wordCountStateStore.persistent());

    //input
    ConsumerRecordFactory<String, String> recordFactory = new ConsumerRecordFactory<>(Topics.TEXT_LINE, new StringSerializer(), new StringSerializer());
    testDriver.pipeInput(recordFactory.create("Hello Kafka Stream World", 10000));
    testDriver.pipeInput(recordFactory.create("Welcome to Kafka World", 20000));
    testDriver.pipeInput(recordFactory.create("Bye Bye to Kafka!", 50000));

    //output
    Map<String, Long> outputWordCountMap = new HashMap<>();
    ProducerRecord<String, Long> outputRecord;
    while (null != (outputRecord = testDriver.readOutput(Topics.WORD_COUNT_OUTPUT, new StringDeserializer(), new LongDeserializer()))) {
      logger.info("Key: {}, Value: {}", outputRecord.key(), outputRecord.value());

      outputWordCountMap.put(outputRecord.key(), outputRecord.value());
    }

    //verify
    Assert.assertEquals(expectedWordCountMap, outputWordCountMap);
  }

  @Test
  public void shouldCountWordByTumblingWindowsOf10Secs() {
    testDriver = new TopologyTestDriver(WordCountStreamApp.createWindowedTopology(builder, 10000).build(), props);

    //reset state store
    StateStore wordCountStateStore = testDriver.getStateStore("word_count_window_state_store");
    logger.info("state store:{} is persistent:{}", wordCountStateStore.name(), wordCountStateStore.persistent());

    //input
    ConsumerRecordFactory<String, String> recordFactory = new ConsumerRecordFactory<>(Topics.TEXT_LINE, new StringSerializer(), new StringSerializer());
    testDriver.pipeInput(recordFactory.create("Hello Kafka Stream World", 5000));
    testDriver.pipeInput(recordFactory.create("Welcome to Kafka World", 7000));
    testDriver.pipeInput(recordFactory.create("Bye to Kafka World", 15000));

    //output
    Map<String, Long> outputWordCountMap = new HashMap<>();
    ProducerRecord<String, Long> outputRecord;
    while (null != (outputRecord = testDriver.readOutput(Topics.WORD_COUNT_OUTPUT, new StringDeserializer(), new LongDeserializer()))) {
      logger.info("Key: {}, Value: {}", outputRecord.key(), outputRecord.value());

      outputWordCountMap.put(outputRecord.key(), outputRecord.value());
    }

    //verify
    Map<String, Long> expectedWordCountMap = new HashMap<String, Long> (){{
      put("hello@0/10000", 1L);
      put("kafka@0/10000", 2L);
      put("stream@0/10000", 1L);
      put("world@0/10000", 2L);
      put("welcome@0/10000", 1L);
      put("to@0/10000", 1L);
      put("bye@10000/20000", 1L);
      put("to@10000/20000", 1L);
      put("kafka@10000/20000", 1L);
      put("world@10000/20000", 1L);
    }};

    Assert.assertEquals(expectedWordCountMap, outputWordCountMap);
  }
}
