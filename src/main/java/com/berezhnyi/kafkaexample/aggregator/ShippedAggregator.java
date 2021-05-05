package com.berezhnyi.kafkaexample.aggregator;

import com.berezhnyi.kafkaexample.model.ResultingEvent;
import com.berezhnyi.kafkaexample.model.ShippedEvent;
import org.apache.kafka.streams.kstream.Aggregator;

public class ShippedAggregator implements Aggregator<String, ShippedEvent, ResultingEvent> {
    @Override
    public ResultingEvent apply(final String appId,
                                final ShippedEvent shippedEvent,
                                final ResultingEvent resultingEvent) {
        resultingEvent.setCorrelationId(shippedEvent.getCorrelationId());
        resultingEvent.setSomeField2(shippedEvent.getSomeField2());
        return resultingEvent;
    }
}
