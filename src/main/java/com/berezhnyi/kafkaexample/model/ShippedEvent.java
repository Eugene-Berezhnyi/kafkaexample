package com.berezhnyi.kafkaexample.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ShippedEvent {
    public String correlationId;
    public String someField2;
    private long timestamp;
}
