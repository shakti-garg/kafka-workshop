package com.max_value_per_key;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;

public class CustomMaxAggregatorSupplier implements ProcessorSupplier<String, Long>{
  @Override
  public Processor<String, Long> get() {
    return new CustomMaxAggregator();
  }
}

class CustomMaxAggregator implements Processor<String, Long> {
  ProcessorContext context;
  private KeyValueStore<String, Long> store;

  @Override
  public void init(ProcessorContext context) {
    this.context = context;
    context.schedule(Duration.ofMillis(60000), PunctuationType.WALL_CLOCK_TIME, time -> flushStore());
    context.schedule(Duration.ofMillis(10000), PunctuationType.STREAM_TIME, time -> flushStore());

    store = (KeyValueStore<String, Long>) context.getStateStore("aggStore");
  }

  @Override
  public void process(String key, Long value) {
    Long oldValue = store.get(key);
    if (oldValue == null || value > oldValue) {
      store.put(key, value);
    }
  }

  private void flushStore() {
    KeyValueIterator<String, Long> it = store.all();
    while (it.hasNext()) {
      KeyValue<String, Long> next = it.next();
      context.forward(next.key, next.value);
    }
  }

  @Override
  public void close() {}
}