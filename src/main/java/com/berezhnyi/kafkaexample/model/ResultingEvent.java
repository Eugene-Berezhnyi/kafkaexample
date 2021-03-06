package com.berezhnyi.kafkaexample.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ResultingEvent {
    public String correlationId;
    public String someField1;
    public String someField2;
    public String someField3;
}
