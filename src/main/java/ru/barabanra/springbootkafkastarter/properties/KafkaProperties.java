package ru.barabanra.springbootkafkastarter.properties;

import lombok.Data;

@Data
public class KafkaProperties {

    private String bootstrapAddress;

    private String groupId;

}
