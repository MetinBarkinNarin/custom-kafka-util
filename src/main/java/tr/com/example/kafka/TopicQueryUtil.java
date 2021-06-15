package tr.com.example.kafka;

import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

public final class TopicQueryUtil {
    private TopicQueryUtil() {
    }

    public static <K, V> KafkaReceiver<K, V> reactiveKafkaReceiver(String topicName, Map<String, Object> consumerProps) {
        ReceiverOptions<K, V> receiverOptions = createReceiverOptions(topicName, consumerProps);
        return KafkaReceiver.create(receiverOptions);
    }

    public static TopicRecord<?, ?> convert(ReceiverRecord<byte[], byte[]> record) {
        try {
            Object key = new String(record.key());
            Object value = convertToObject(record.value());
            return new TopicRecord<>(key, value);
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    static <K, V> ReceiverOptions<K, V> createReceiverOptions(String topicName, Map<String, Object> consumerProps) {
        return ReceiverOptions
                .<K, V>create(consumerProps)
                .subscription(Collections.singleton(topicName));
    }

    static Object convertToObject(byte[] bytes) throws IOException {
        return ObjectMapperUtil.convert(bytes, Object.class);
    }


//    private static ReceiverRecord<?, ?> getReceiverRecord(ReceiverRecord<?, ?> record, Object key, Object value) {
//        ConsumerRecord<Object, Object> consumerRecord = new ConsumerRecord<>(
//                record.topic(),
//                record.partition(),
//                record.offset(),
//                record.timestamp(),
//                record.timestampType(),
//                record.checksum(),
//                record.serializedKeySize(),
//                record.serializedValueSize(),
//                key,
//                value,
//                record.headers(),
//                record.leaderEpoch()
//        );
//        return new ReceiverRecord<>( consumerRecord, record.receiverOffset());
//    }
}
