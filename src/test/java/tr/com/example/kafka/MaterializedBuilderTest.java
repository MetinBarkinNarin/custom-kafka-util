package tr.com.example.kafka;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.processor.StateStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import tr.com.example.kafka.MaterializedBuilder.StorageType;

import java.util.Hashtable;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static tr.com.example.kafka.CustomKafkaTestUtils.*;

class MaterializedBuilderTest {

    private static final Serde<String> STRING_KEY_SERDE = Serdes.String();
    private static final Serde<Long> LONG_VALUE_SERDE = Serdes.Long();

    private Materializer<String, Long> materializer;

    private TopologyTestDriver topologyTestDriver;

    @AfterEach
    void tearDown() throws Exception {
        close(topologyTestDriver);
    }

    @Test
    void bothStoreNameAndStorageTypeAreNull() {
        materializer = new MaterializedBuilder<>(STRING_KEY_SERDE, LONG_VALUE_SERDE)
                .build();
        topologyTestDriver = anExample("bothStoreNameAndStorageTypeAreNull", materializer);
        Map<String, StateStore> allStateStores = topologyTestDriver.getAllStateStores();
        assertThat(allStateStores)
                .isNotNull()
                .hasSize(1);

        assertThat(getStore(allStateStores))
                .isPresent()
                .map(StateStore::name)
                .isPresent()
                .hasValueSatisfying(name ->
                        assertThat(name)
                                .startsWith("KSTREAM-AGGREGATE-STATE-STORE")
                                .endsWith("1")
                );
    }

    @Test
    void storeNameIsValidButNoStorageType() {
        final String storeName = "custom-store-name"; // + UUID.randomUUID().toString();
        materializer = new MaterializedBuilder<>(STRING_KEY_SERDE, LONG_VALUE_SERDE)
                .storeName(storeName)
                .build();
        TopologyTestDriver topologyTestDriver = anExample("storeNameIsValidButNoStorageType", materializer);

        Map<String, StateStore> allStateStores = topologyTestDriver.getAllStateStores();
        assertThat(allStateStores)
                .isNotNull()
                .hasSize(1);
        assertThat(getStore(allStateStores))
                .isPresent()
                .map(StateStore::name)
                .isPresent()
                .hasValueSatisfying(name -> assertThat(name).startsWith(storeName));
    }

    @Test
    void persistentStorage() {
        StorageType persistent = StorageType.PERSISTENT;
        final String storeName = persistent.name() + "-store-name";
        materializer = new MaterializedBuilder<>(STRING_KEY_SERDE, LONG_VALUE_SERDE)
                .withPersistent(storeName)
                .build();
        byStorageType(storeName, Boolean.TRUE);
    }

    @Test
    void inMemoryStorage() {
        StorageType inMemory = StorageType.IN_MEMORY;
        final String storeName = inMemory.name() + "-store-name";

        materializer = new MaterializedBuilder<>(STRING_KEY_SERDE, LONG_VALUE_SERDE)
                .withInMemory(storeName)
                .build();
        byStorageType(storeName, Boolean.FALSE);
    }

    @Test
    void throwNullPointerExceptionStoreNameIsInvalidForPersistentStorage() {
        assertThrows(
                NullPointerException.class,
                () -> new MaterializedBuilder<>(STRING_KEY_SERDE, LONG_VALUE_SERDE)
                        .withPersistent(null)
                        .build()
        );
    }

    @Test
    void throwNullPointerExceptionStoreNameIsInvalidForInMemoryStorage() {
        assertThrows(
                NullPointerException.class,
                () -> new MaterializedBuilder<>(STRING_KEY_SERDE, LONG_VALUE_SERDE)
                        .withInMemory(null)
                        .build()
        );
    }

    private void byStorageType(String storeName, Boolean expectedValue) {
        topologyTestDriver = anExample(storeName, materializer);
        Map<String, StateStore> allStateStores = topologyTestDriver.getAllStateStores();
        assertThat(allStateStores)
                .isNotNull()
                .hasSize(1);
        assertThat(getStore(allStateStores))
                .isPresent()
                .hasValueSatisfying(stateStore -> {
                    assertThat(stateStore.name()).startsWith(storeName);
                    assertThat(stateStore.persistent()).isEqualTo(expectedValue);
                });
    }

    private TopologyTestDriver anExample(String applicationId, Materializer<String, Long> materializer) {
        final String topicName = "materialized-topic";
        Map<String, Object> propertiesMap = new CustomStreamsConfigBuilder()
                .applicationId(applicationId)
                .bootstrapServers("anExample config")
                .autoOffset("earliest")
                .build();
        Properties properties = propertiesMap
                .entrySet()
                .stream()
                .collect(
                        Properties::new,
                        (p, e) -> p.put(e.getKey(), e.getValue()),
                        Hashtable::putAll
                );

        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, Long> inputStream = toStream(builder, topicName, Serdes.String(), Serdes.Long());

        inputStream.groupByKey()
                .aggregate(() -> 0L,
                        (k, v, a) -> a + 1,
                        materializer.get()
                );

        final TopologyTestDriver topologyTestDriver = topologyTestDriver(builder, properties);
        produceKeyValuesSynchronously(
                topicName,
                LongStream.rangeClosed(1, 10).mapToObj(i -> new KeyValue<>("" + i, i)).collect(Collectors.toList()),
                topologyTestDriver,
                Serdes.String().serializer(),
                Serdes.Long().serializer()
        );
        return topologyTestDriver;
    }

    private Optional<StateStore> getStore(Map<String, StateStore> stateStores) {
        return stateStores.values().stream().findFirst();
    }

}
