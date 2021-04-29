package com.berezhnyi.kafkaexample.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class OrderCreatedEvent {
    public String correlationId;
    public String someField1;
    private long timestamp;
}
