package ru.barabanra.springbootkafkastarter.factory;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;
import ru.barabanra.springbootkafkastarter.interceptor.LoggingProducerInterceptor;
import ru.barabanra.springbootkafkastarter.properties.KafkaProperties;

import java.util.HashMap;
import java.util.Map;

@RequiredArgsConstructor
public class KafkaProducerTemplateFactory<T> {

    private final KafkaProperties kafkaProperties;
    private final ObjectMapper objectMapper;

    public ProducerFactory<String, T> build() {
        return new DefaultKafkaProducerFactory<>(
                this.configProps(),
                new StringSerializer(),
                new JsonSerializer<>(objectMapper));
    }

    private Map<String, Object> configProps() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapAddress());
        configProps.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, LoggingProducerInterceptor.class);
        return configProps;
    }

}
