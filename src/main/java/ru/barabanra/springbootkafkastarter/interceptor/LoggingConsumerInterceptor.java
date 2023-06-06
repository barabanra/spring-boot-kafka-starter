package ru.barabanra.springbootkafkastarter.interceptor;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import ru.barabanra.springbootkafkastarter.utils.KafkaHeaderUtils;

import java.util.Map;

@Slf4j
public class LoggingConsumerInterceptor<K, V> implements ConsumerInterceptor<K, V> {

    private final static ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Override
    @SneakyThrows
    public ConsumerRecords<K, V> onConsume(ConsumerRecords<K, V> records) {
        for (ConsumerRecord<K, V> record : records) {
            KafkaHeaderUtils.refreshMdc(record.headers());

            log.debug("[Kafka][Consumer] topic - {}, offset - {}, headers - {}," +
                            " partition - {}, key - {}, value - {}",
                    record.topic(),
                    record.offset(),
                    KafkaHeaderUtils.buildHeaderString(record.headers()),
                    record.partition(),
                    OBJECT_MAPPER.writeValueAsString(record.key()),
                    OBJECT_MAPPER.writeValueAsString(record.value()));

            KafkaHeaderUtils.clearMdc();
        }
        return records;
    }

    @Override
    public void onCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {
    }

    @Override
    public void close() {
    }

    @Override
    public void configure(Map<String, ?> configs) {
    }
}
