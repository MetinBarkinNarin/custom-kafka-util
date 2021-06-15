package tr.com.example.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;


class CustomConsumerConfigBuilderTest {

    @Test
    void constructEmptyMap() {
        Map<String, Object> properties = new CustomConsumerConfigBuilder().build();

        assertThat(properties)
                .isNotNull()
                .isEmpty();
    }

    @Test
    void includesEntryForValidClientId() {
        TestHelper.testEqual(
                "client-id",
                ConsumerConfig.CLIENT_ID_CONFIG,
                this::clientId
        );
    }

    @Test
    void notIncludeEntryForInvalidClientId() {
        TestHelper.testEqual(
                null,
                ConsumerConfig.CLIENT_ID_CONFIG,
                this::clientId
        );
    }

    @Test
    void includesEntryForValidGroupId() {
        TestHelper.testEqual(
                "group-id",
                ConsumerConfig.GROUP_ID_CONFIG,
                this::groupId
        );
    }

    @Test
    void notIncludeEntryForInvalidGroupId() {
        TestHelper.testEqual(
                null,
                ConsumerConfig.GROUP_ID_CONFIG,
                this::groupId
        );
    }

    @Test
    void includesEntryForValidBootstrapServers() {
        TestHelper.testEqual(
                "http://localhost:9092",
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                this::bootstrapServers
        );
    }

    @Test
    void notIncludeEntryForInvalidBootstrapServers() {
        TestHelper.testEqual(
                null,
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                this::bootstrapServers
        );
    }

    @Test
    void includesEntryForValidAutoOffset() {
        TestHelper.testEqual(
                "earliest",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                this::autoOffset
        );
    }

    @Test
    void notIncludeEntryForInvalidAutoOffset() {
        TestHelper.testEqual(
                null,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                this::autoOffset
        );
    }

    @Test
    void includesEntryForValidKeyDeserializer() {
        TestHelper.testEqual(
                Serdes.String().deserializer().getClass(),
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                this::keyDeserializer
        );
    }

    @Test
    void notIncludeEntryForInvalidKeyDeserializer() {
        TestHelper.testEqual(
                null,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                this::keyDeserializer
        );
    }

    @Test
    void includesEntryForValidValueDeserializer() {
        TestHelper.testEqual(
                Serdes.Long().deserializer().getClass(),
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                this::valueDeserializer
        );
    }

    @Test
    void notIncludeEntryForInvalidValueDeserializer() {
        TestHelper.testEqual(
                null,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                this::valueDeserializer
        );
    }

    @Test
    void includesEntryForTrustedPackages() {
        TestHelper.testEqual(
                "java.util",
                JsonDeserializer.TRUSTED_PACKAGES,
                this::trustedPackages
        );
    }

    @Test
    void notIncludeEntryForTrustedPackages() {
        TestHelper.testEqual(
                null,
                JsonDeserializer.TRUSTED_PACKAGES,
                this::trustedPackages
        );
    }

    @Test
    void includesEntryForAutoCreateTopics() {
        TestHelper.testEqual(
                false,
                ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG,
                this::autoCreateTopics
        );
    }

    @Test
    void notIncludeEntryForAutoCreateTopics() {
        TestHelper.testEqual(
                null,
                ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG,
                this::autoCreateTopics
        );
    }

    private Map<String, Object> clientId(String clientId) {
        return new CustomConsumerConfigBuilder()
                .clientId(clientId)
                .build();
    }

    private Map<String, Object> groupId(String groupId) {
        return new CustomConsumerConfigBuilder()
                .groupId(groupId)
                .build();
    }

    private Map<String, Object> bootstrapServers(String bootstrapServers) {
        return new CustomConsumerConfigBuilder()
                .bootstrapServers(bootstrapServers)
                .build();
    }

    private Map<String, Object> autoOffset(String autoOffset) {
        return new CustomConsumerConfigBuilder()
                .autoOffset(autoOffset)
                .build();
    }

    private Map<String, Object> keyDeserializer(Class<?> keyDeserializerClass) {
        return new CustomConsumerConfigBuilder()
                .keyDeserializer(keyDeserializerClass)
                .build();
    }

    private Map<String, Object> valueDeserializer(Class<?> valueDeserializerClass) {
        return new CustomConsumerConfigBuilder()
                .valueDeserializer(valueDeserializerClass)
                .build();
    }

    private Map<String, Object> trustedPackages(String trustedPackages) {
        return new CustomConsumerConfigBuilder()
                .trustedPackages(trustedPackages)
                .build();
    }

    private Map<String, Object> autoCreateTopics(Boolean autoCreateTopics) {
        return new CustomConsumerConfigBuilder()
                .autoCreateTopics(autoCreateTopics)
                .build();
    }
}
