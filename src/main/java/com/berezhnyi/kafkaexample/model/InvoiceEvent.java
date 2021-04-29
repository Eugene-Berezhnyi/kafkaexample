package com.berezhnyi.kafkaexample.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class InvoiceEvent {
    public String correlationId;
    public String someField3;
    private long timestamp;
}
