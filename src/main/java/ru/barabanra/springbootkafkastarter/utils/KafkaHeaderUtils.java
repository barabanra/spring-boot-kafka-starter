package ru.barabanra.springbootkafkastarter.utils;

import com.google.common.collect.ImmutableList;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.slf4j.MDC;

import java.util.stream.Collectors;

public class KafkaHeaderUtils {

    public static String buildHeaderString(Headers headers) {
        return ImmutableList.copyOf(headers.iterator())
                .stream()
                .map(header -> String.format("[%s:%s]", header.key(), new String(header.value())))
                .collect(Collectors.joining(","));
    }

    public static void refreshMdc(Headers headers) {
        String[] traceIdSpanId = KafkaHeaderUtils.getB3s(headers);

        if (traceIdSpanId.length == 3) {
            String traceId = traceIdSpanId[0];
            String spanId = traceIdSpanId[1];
            MDC.put("traceId", traceId);
            MDC.put("spanId", spanId);
        }
    }

    private static String[] getB3s(Headers headers) {
        return ImmutableList.copyOf(headers.headers("b3"))
                .stream()
                .findFirst()
                .map(Header::value)
                .map(String::new)
                .map(header -> header.split("-"))
                .orElseGet(() -> new String[]{});
    }

    public static void clearMdc() {
        MDC.clear();
    }

}
