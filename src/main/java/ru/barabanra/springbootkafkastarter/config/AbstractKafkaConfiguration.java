package ru.barabanra.springbootkafkastarter.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import ru.barabanra.springbootkafkastarter.factory.KafkaConsumerFactory;
import ru.barabanra.springbootkafkastarter.factory.KafkaProducerTemplateFactory;
import ru.barabanra.springbootkafkastarter.properties.KafkaProperties;

@RequiredArgsConstructor
public class AbstractKafkaConfiguration<V> {

    private final KafkaProperties kafkaProperties;
    private final ObjectMapper objectMapper;

    public ProducerFactory<String, V> producerFactory() {
        return new KafkaProducerTemplateFactory<V>(
                kafkaProperties, objectMapper)
                .build();
    }

    public KafkaTemplate<String, V> kafkaTemplate(
            ProducerFactory<String, V> testKafkaPayloadKafkaTemplate) {
        return new KafkaTemplate<>(testKafkaPayloadKafkaTemplate);
    }

    public ConcurrentKafkaListenerContainerFactory<String, V> containerFactory() {
        return new KafkaConsumerFactory<V>(
                kafkaProperties, objectMapper)
                .build();
    }

}
