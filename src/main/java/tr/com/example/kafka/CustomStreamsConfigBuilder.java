package tr.com.example.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.StreamsConfig;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class CustomStreamsConfigBuilder {
    private String applicationId;
    private String bootstrapServers;
    private String autoOffset;
    private String stateDirectoryConfig;
    private Class<?> keyClass;
    private Class<?> valueClass;


    public CustomStreamsConfigBuilder applicationId(String applicationId) {
        this.applicationId = applicationId;
        return this;
    }

    public CustomStreamsConfigBuilder bootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
        return this;
    }

    public CustomStreamsConfigBuilder autoOffset(String autoOffset) {
        this.autoOffset = autoOffset;
        return this;
    }

    public CustomStreamsConfigBuilder defaultKeySerde(Class<?> keyClass) {
        this.keyClass = keyClass;
        return this;
    }

    public CustomStreamsConfigBuilder defaultValueSerde(Class<?> valueClass) {
        this.valueClass = valueClass;
        return this;
    }

    public CustomStreamsConfigBuilder stateDirectoryConfig(String stateDirectoryConfig) {
        this.stateDirectoryConfig = stateDirectoryConfig;
        return this;
    }

    public Map<String, Object> build() {
        final Map<String, Object> propertiesMap = new HashMap<>();
        if (Objects.nonNull(applicationId))
            propertiesMap.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        if (Objects.nonNull(bootstrapServers))
            propertiesMap.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        if (Objects.nonNull(autoOffset))
            propertiesMap.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffset);
        if (Objects.nonNull(stateDirectoryConfig))
            propertiesMap.put(StreamsConfig.STATE_DIR_CONFIG, stateDirectoryConfig); // TestUtils.tempDirectory().getAbsolutePath());
        if (Objects.nonNull(keyClass))
            propertiesMap.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, keyClass);
        if (Objects.nonNull(valueClass))
            propertiesMap.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, valueClass);
        return propertiesMap;
    }
}
