package com.berezhnyi.kafkaexample.aggregator;

import com.berezhnyi.kafkaexample.model.OrderCreatedEvent;
import com.berezhnyi.kafkaexample.model.ResultingEvent;
import org.apache.kafka.streams.kstream.Aggregator;

public class OrderCreatedAggregator implements Aggregator<String, OrderCreatedEvent, ResultingEvent> {
    @Override
    public ResultingEvent apply(final String appId,
                             final OrderCreatedEvent orderCreatedEvent,
                             final ResultingEvent resultingEvent) {
        resultingEvent.setCorrelationId(orderCreatedEvent.getCorrelationId());
        resultingEvent.setSomeField1(orderCreatedEvent.getSomeField1());
        return resultingEvent;
    }
}
