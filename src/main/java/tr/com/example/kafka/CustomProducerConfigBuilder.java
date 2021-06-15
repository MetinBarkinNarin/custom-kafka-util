package tr.com.example.kafka;

import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class CustomProducerConfigBuilder {
    private String bootstrapServers;
    private Class<?> keySerializer;
    private Class<?> valueSerializer;


    public CustomProducerConfigBuilder bootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
        return this;
    }

    public CustomProducerConfigBuilder keySerializer(Class<?> keySerializer) {
        this.keySerializer = keySerializer;
        return this;
    }

    public CustomProducerConfigBuilder valueSerializer(Class<?> valueSerializer) {
        this.valueSerializer = valueSerializer;
        return this;
    }

    public Map<String, Object> build() {
        final Map<String, Object> propertiesMap = new HashMap<>();
        if (Objects.nonNull(bootstrapServers))
            propertiesMap.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        if (Objects.nonNull(keySerializer))
            propertiesMap.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializer);
        if (Objects.nonNull(valueSerializer))
            propertiesMap.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer);
        return propertiesMap;
    }
}
