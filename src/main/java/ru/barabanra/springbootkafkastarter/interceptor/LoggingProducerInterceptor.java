package ru.barabanra.springbootkafkastarter.interceptor;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import ru.barabanra.springbootkafkastarter.utils.KafkaHeaderUtils;

import java.util.Map;

@Slf4j
public class LoggingProducerInterceptor<K, V> implements ProducerInterceptor<K, V> {

    private final static ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Override
    @SneakyThrows
    public ProducerRecord<K, V> onSend(ProducerRecord<K, V> record) {
        log.debug("[Kafka][Producer] topic - {}, headers - {}, partition - {}, key - {}, value - {}",
                record.topic(),
                KafkaHeaderUtils.buildHeaderString(record.headers()),
                record.partition(),
                OBJECT_MAPPER.writeValueAsString(record.key()),
                OBJECT_MAPPER.writeValueAsString(record.value()));

        return record;
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
    }

    @Override
    public void close() {
    }

    @Override
    public void configure(Map<String, ?> configs) {
    }
}
