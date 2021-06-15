package tr.com.example.kafka;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static tr.com.example.kafka.TestHelper.testEqual;


class CustomProducerConfigBuilderTest {

    @Test
    void constructEmptyMap() {
        Map<String, Object> properties = new CustomProducerConfigBuilder().build();

        assertThat(properties)
                .isNotNull()
                .isEmpty();
    }

    @Test
    void includesEntryForValidBootstrapServers() {
        testEqual(
                "http://localhost:9092",
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                this::bootstrapServers
        );
    }

    @Test
    void notIncludeEntryForInvalidBootstrapServers() {
        testEqual(
                null,
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                this::bootstrapServers
        );
    }

    @Test
    void includesEntryForValidKeySerializer() {
        testEqual(
                Serdes.String().serializer().getClass(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                this::keySerializer
        );
    }

    @Test
    void notIncludeEntryForInvalidKeySerializer() {
        testEqual(
                null,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                this::keySerializer
        );
    }

    @Test
    void includesEntryForValidValueSerializer() {
        testEqual(
                Serdes.Long().serializer().getClass(),
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                this::valueSerializer
        );
    }

    @Test
    void notIncludeEntryForInvalidValueSerializer() {
        testEqual(
                null,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                this::valueSerializer
        );
    }

    private Map<String, Object> bootstrapServers(String bootstrapServers) {
        return new CustomProducerConfigBuilder()
                .bootstrapServers(bootstrapServers)
                .build();
    }

    private Map<String, Object> keySerializer(Class<?> keySerializerClass) {
        return new CustomProducerConfigBuilder()
                .keySerializer(keySerializerClass)
                .build();
    }

    private Map<String, Object> valueSerializer(Class<?> valueSerializerClass) {
        return new CustomProducerConfigBuilder()
                .valueSerializer(valueSerializerClass)
                .build();
    }
}
