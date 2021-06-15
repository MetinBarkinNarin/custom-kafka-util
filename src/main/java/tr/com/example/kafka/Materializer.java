package tr.com.example.kafka;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;

/**
 * Created by sotlu on 27.08.2019.
 */
public interface Materializer<K, V> {
    Materialized<K, V, KeyValueStore<Bytes, byte[]>> get();
}
