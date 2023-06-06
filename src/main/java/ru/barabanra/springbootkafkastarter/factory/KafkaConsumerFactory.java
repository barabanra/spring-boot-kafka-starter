package ru.barabanra.springbootkafkastarter.factory;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.converter.JsonMessageConverter;
import ru.barabanra.springbootkafkastarter.interceptor.LoggingConsumerInterceptor;
import ru.barabanra.springbootkafkastarter.properties.KafkaProperties;

import java.util.HashMap;
import java.util.Map;

@RequiredArgsConstructor
public class KafkaConsumerFactory<T> {

    private final KafkaProperties kafkaProperties;
    private final ObjectMapper objectMapper;

    public ConcurrentKafkaListenerContainerFactory<String, T> build() {
        ConcurrentKafkaListenerContainerFactory<String, T> containerFactory = new ConcurrentKafkaListenerContainerFactory<>();
        containerFactory.setMessageConverter(new JsonMessageConverter(objectMapper));
        containerFactory.setConsumerFactory(new DefaultKafkaConsumerFactory<>(this.configProps()));
        return containerFactory;
    }

    private Map<String, Object> configProps() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapAddress());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, kafkaProperties.getGroupId());

        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, LoggingConsumerInterceptor.class);
        return props;
    }

}
