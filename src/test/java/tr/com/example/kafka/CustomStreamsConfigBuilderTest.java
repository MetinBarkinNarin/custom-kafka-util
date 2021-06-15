package tr.com.example.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;


class CustomStreamsConfigBuilderTest {

    @Test
    void constructEmptyMap() {
        Map<String, Object> propertiesMap = new CustomStreamsConfigBuilder().build();

        assertThat(propertiesMap)
                .isNotNull()
                .isEmpty();
    }

    @Test
    void includesEntryForValidApplicationId() {
        TestHelper.testEqual(
                "application-id",
                StreamsConfig.APPLICATION_ID_CONFIG,
                this::applicationId
        );
    }

    @Test
    void notIncludeEntryForInvalidApplicationId() {
        TestHelper.testEqual(
                null,
                StreamsConfig.APPLICATION_ID_CONFIG,
                this::applicationId
        );
    }

    @Test
    void includesEntryForValidBootstrapServers() {
        TestHelper.testEqual(
                "http://localhost:9092",
                StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                this::bootstrapServers
        );
    }

    @Test
    void notIncludeEntryForInvalidBootstrapServers() {
        TestHelper.testEqual(
                null,
                StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
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
    void includesEntryForValidStateDirectory() {
        TestHelper.testEqual(
                "/mummy1/mummy2/",
                StreamsConfig.STATE_DIR_CONFIG,
                this::stateDirectoryConfig
        );
    }

    @Test
    void notIncludeEntryForInvalidStateDirectory() {
        TestHelper.testEqual(
                null,
                StreamsConfig.STATE_DIR_CONFIG,
                this::stateDirectoryConfig
        );
    }

    @Test
    void includesEntryForValidKeySerde() {
        TestHelper.testEqual(
                Serdes.String().getClass(),
                StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
                this::defaultKeySerde
        );
    }

    @Test
    void notIncludeEntryForInvalidKeySerde() {
        TestHelper.testEqual(
                null,
                StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
                this::defaultKeySerde
        );
    }

    @Test
    void includesEntryForValidValueSerde() {
        TestHelper.testEqual(
                Serdes.Long().getClass(),
                StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
                this::defaultValueSerde
        );
    }

    @Test
    void notIncludeEntryForInvalidValueSerde() {
        TestHelper.testEqual(
                null,
                StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
                this::defaultValueSerde
        );
    }

    private Map<String, Object> applicationId(String applicationId) {
        return new CustomStreamsConfigBuilder()
                .applicationId(applicationId)
                .build();
    }

    private Map<String, Object> bootstrapServers(String bootstrapServers) {
        return new CustomStreamsConfigBuilder()
                .bootstrapServers(bootstrapServers)
                .build();
    }

    private Map<String, Object> autoOffset(String autoOffset) {
        return new CustomStreamsConfigBuilder()
                .autoOffset(autoOffset)
                .build();
    }

    private Map<String, Object> stateDirectoryConfig(String stateDirectoryConfig) {
        return new CustomStreamsConfigBuilder()
                .stateDirectoryConfig(stateDirectoryConfig)
                .build();
    }

    private Map<String, Object> defaultKeySerde(Class<?> defaultKeySerdeClass) {
        return new CustomStreamsConfigBuilder()
                .defaultKeySerde(defaultKeySerdeClass)
                .build();
    }

    private Map<String, Object> defaultValueSerde(Class<?> defaultValueSerdeClass) {
        return new CustomStreamsConfigBuilder()
                .defaultValueSerde(defaultValueSerdeClass)
                .build();
    }
}
