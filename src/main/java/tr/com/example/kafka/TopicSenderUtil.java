package tr.com.example.kafka;

import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;

import java.util.Map;

final public class TopicSenderUtil {
    private TopicSenderUtil() {
    }

    public static <K, V> KafkaSender<K, V> reactiveKafkaSender(Map<String, Object> producerProps) {
        SenderOptions<K, V> senderOptions = createSenderOptions(producerProps);
        return KafkaSender.create(senderOptions);
    }

    private static <K, V> SenderOptions<K, V> createSenderOptions(Map<String, Object> producerProps) {
        return SenderOptions
                .<K, V>create(producerProps)
                .maxInFlight(1024);
    }
}
