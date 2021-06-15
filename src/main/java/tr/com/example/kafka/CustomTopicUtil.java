package tr.com.example.kafka;

import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Optional;

public final class CustomTopicUtil {

    public static void throwExceptionIfTopicDoesNotExist(String topicName,
                                                         KafkaConsumer<?, ?> kafkaConsumer) {
        Optional.of(topicExists(topicName, kafkaConsumer))
                .filter(Boolean.TRUE::equals)
                .orElseThrow(() -> new TopicNotFoundException(topicName));
    }

    public static boolean topicExists(String topicName,
                                      KafkaConsumer<?, ?> kafkaConsumer) {
        return kafkaConsumer
                .listTopics()
                .keySet()
                .stream()
                .anyMatch(t -> t.equalsIgnoreCase(topicName));
    }


}
