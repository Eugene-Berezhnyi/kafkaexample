package com.berezhnyi.kafkaexample.serds;

public class DagSerde {
    public static OrderCreatedEventSerde CREATE_SERDE = new OrderCreatedEventSerde();
    public static ShippedEventSerde SHIPPED_SERDE = new ShippedEventSerde();
    public static InvoiceEventSerde INVOICE_SERDE = new InvoiceEventSerde();
    public static ResultingEventSerde RESULTING_SERDE = new ResultingEventSerde();
}
