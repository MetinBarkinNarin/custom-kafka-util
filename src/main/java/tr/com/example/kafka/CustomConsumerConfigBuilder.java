package tr.com.example.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class CustomConsumerConfigBuilder {
    private String clientId;
    private String groupId;
    private String bootstrapServers;
    private String autoOffset;
    private String trustedPackages;
    private Boolean autoCreateTopics;
    private Class<?> keyDeserializer;
    private Class<?> valueDeserializer;


    public CustomConsumerConfigBuilder clientId(String clientId) {
        this.clientId = clientId;
        return this;
    }

    public CustomConsumerConfigBuilder groupId(String groupId) {
        this.groupId = groupId;
        return this;
    }

    public CustomConsumerConfigBuilder bootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
        return this;
    }

    public CustomConsumerConfigBuilder autoOffset(String autoOffset) {
        this.autoOffset = autoOffset;
        return this;
    }

    public CustomConsumerConfigBuilder keyDeserializer(Class<?> keyDeserializer) {
        this.keyDeserializer = keyDeserializer;
        return this;
    }

    public CustomConsumerConfigBuilder valueDeserializer(Class<?> valueDeserializer) {
        this.valueDeserializer = valueDeserializer;
        return this;
    }

    public CustomConsumerConfigBuilder trustedPackages(String trustedPackages) {
        this.trustedPackages = trustedPackages;
        return this;
    }

    public CustomConsumerConfigBuilder autoCreateTopics(Boolean autoCreateTopics) {
        this.autoCreateTopics = autoCreateTopics;
        return this;
    }

    public Map<String, Object> build() {
        final Map<String, Object> propertiesMap = new HashMap<>();
        if (Objects.nonNull(clientId))
            propertiesMap.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
        if (Objects.nonNull(groupId))
            propertiesMap.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        if (Objects.nonNull(bootstrapServers))
            propertiesMap.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        if (Objects.nonNull(autoOffset))
            propertiesMap.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffset);
        if (Objects.nonNull(trustedPackages))
            propertiesMap.put(JsonDeserializer.TRUSTED_PACKAGES, trustedPackages);
        if (Objects.nonNull(keyDeserializer))
            propertiesMap.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer);
        if (Objects.nonNull(valueDeserializer))
            propertiesMap.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer);
        if (Objects.nonNull(autoCreateTopics))
            propertiesMap.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, autoCreateTopics);
        return propertiesMap;
    }
}
