package tr.com.example.kafka;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Serdes;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverRecord;

import java.util.Map;
import java.util.UUID;

@Component
public class CustomKafkaReceiver {

    private KafkaProperties kafkaProperties;
    private KafkaConsumer<?, ?> kafkaConsumer;

    public CustomKafkaReceiver(KafkaProperties kafkaProperties) {
        this.kafkaProperties = kafkaProperties;
        kafkaConsumer = new KafkaConsumer<>(this.kafkaProperties.buildConsumerProperties());
    }

    public Flux<ReceiverRecord<byte[], byte[]>> receiveFromKafka(String topicName, String offset, String groupIdPrefix) {
        KafkaReceiver<byte[], byte[]> kafkaReceiver = TopicQueryUtil.reactiveKafkaReceiver(topicName, createConsumerProps(offset, groupIdPrefix));
        return kafkaReceiver.receive();
    }

    public boolean topicExists(String topicName){
        return CustomTopicUtil.topicExists(topicName, kafkaConsumer);
    }

    private Map<String, Object> createConsumerProps(String offset, String groupIdPrefix) {
        return new CustomConsumerConfigBuilder()
                .bootstrapServers(String.join(",", kafkaProperties.getBootstrapServers()))
                .groupId(groupIdPrefix + UUID.randomUUID().toString())
                .keyDeserializer(Serdes.ByteArray().deserializer().getClass())
                .valueDeserializer(Serdes.ByteArray().deserializer().getClass())
                .autoOffset(offset)
                .trustedPackages("*")
                .autoCreateTopics(false)
                .build();
    }
}
