package tr.com.example.kafka;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;

import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by sotlu on 28.08.2019.
 */
public final class MaterializedBuilder<K, V> {
    enum StorageType {IN_MEMORY, PERSISTENT}

    private final Serde<K> keySerde;
    private final Serde<V> valueSerde;
    private String storeName;
    private StorageType storageType;


    public MaterializedBuilder(Serde<K> keySerde, Serde<V> valueSerde) {
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
        this.storeName = null;
        this.storageType = null;
    }


    public MaterializedBuilder<K, V> storeName(String storeName) {
        return withInMemory(storeName);
    }

    public MaterializedBuilder<K, V> withInMemory(String storeName) {
        return withStorage(storeName, StorageType.IN_MEMORY);
    }

    public MaterializedBuilder<K, V> withPersistent(String storeName) {
        return withStorage(storeName, StorageType.PERSISTENT);
    }

    private MaterializedBuilder<K, V> withStorage(String storeName, StorageType storageType) {
        this.storeName = storeName;
        this.storageType = storageType;
        return this;
    }

    public Materializer<K, V> build() {
        return Optional
                .ofNullable(this.storageType)
                .map(t -> storeSupplier(storeName, t))
                .map(this::construct)
                .orElseGet(
                        () -> Optional
                                .ofNullable(this.storeName)
                                .map(this::construct)
                                .orElseGet(this::construct)
                );
    }

    private Materializer<K, V> construct(KeyValueBytesStoreSupplier storeSupplier) {
        return () -> Materialized.<K, V>as(storeSupplier)
                .withKeySerde(keySerde)
                .withValueSerde(valueSerde);
    }
    public Materializer<K, V> createMaterialized(String name) {

        return new MaterializedBuilder<K, V>(this.keySerde, this.valueSerde)
                .withPersistent(name)
                .build();
    }

    private Materializer<K, V> construct(String storeName) {
        return () -> Materialized.<K, V, KeyValueStore<Bytes, byte[]>>as(storeName)
                .withKeySerde(keySerde)
                .withValueSerde(valueSerde);
    }

    private Materializer<K, V> construct() {
        return () -> Materialized.with(keySerde, valueSerde);
    }

    private KeyValueBytesStoreSupplier storeSupplier(String storeName, StorageType storageType) {
        switch (storageType) {
            case IN_MEMORY:
                return Stores.inMemoryKeyValueStore(storeName);
            case PERSISTENT:
                return Stores.persistentKeyValueStore(storeName);
            default:
                throw new IllegalArgumentException(
                        "Unhandled " + StorageType.class.getCanonicalName() +
                        "instance should be one of " +
                        Stream.of(StorageType.values()).map(Enum::name).collect(Collectors.joining(", ", "{", "}" )) +
                        "but it was " + storageType);
        }
    }
}
