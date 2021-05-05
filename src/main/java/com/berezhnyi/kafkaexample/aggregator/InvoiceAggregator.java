package com.berezhnyi.kafkaexample.aggregator;

import com.berezhnyi.kafkaexample.model.InvoiceEvent;
import com.berezhnyi.kafkaexample.model.ResultingEvent;
import org.apache.kafka.streams.kstream.Aggregator;

public class InvoiceAggregator implements Aggregator<String, InvoiceEvent, ResultingEvent> {
    @Override
    public ResultingEvent apply(final String appId,
                                final InvoiceEvent invoceEvent,
                                final ResultingEvent resultingEvent) {
        resultingEvent.setCorrelationId(invoceEvent.getCorrelationId());
        resultingEvent.setSomeField3(invoceEvent.getSomeField3());
        return resultingEvent;
    }
}
