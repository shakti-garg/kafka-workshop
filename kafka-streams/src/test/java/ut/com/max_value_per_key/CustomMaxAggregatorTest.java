package com.max_value_per_key;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.MockProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.test.OutputVerifier;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Iterator;

import static org.junit.Assert.*;

public class CustomMaxAggregatorTest {

  private CustomMaxAggregator processorUnderTest = new CustomMaxAggregator();
  private MockProcessorContext context;

  @Before
  public void setup() {
    context = new MockProcessorContext();

    final KeyValueStore<String, Long> store =
        Stores.keyValueStoreBuilder(
            Stores.inMemoryKeyValueStore("aggStore"),
            Serdes.String(), Serdes.Long()).withLoggingDisabled() // Changelog is not supported by MockProcessorContext.
            .build();
    store.init(context, store);
    context.register(store, null);

    processorUnderTest.init(context);
  }

  @Test
  public void shouldFlushStoreForFirstInput() {
    processorUnderTest.process("a", 1L);

    KeyValueStore aggStore = (KeyValueStore)processorUnderTest.context.getStateStore("aggStore");
    Assert.assertEquals(1L, aggStore.get("a"));

    final Iterator<MockProcessorContext.CapturedForward> forwarded = context.forwarded().iterator();
    assertFalse(forwarded.hasNext());

    assertEquals(context.forwarded().size(), 0);
  }

}