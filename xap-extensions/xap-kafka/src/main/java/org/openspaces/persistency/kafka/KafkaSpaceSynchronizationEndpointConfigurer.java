package org.openspaces.persistency.kafka;

import com.gigaspaces.sync.SpaceSynchronizationEndpoint;
import org.apache.kafka.clients.CommonClientConfigs;

import java.util.Map;
import java.util.Properties;

public class KafkaSpaceSynchronizationEndpointConfigurer {
    private SpaceSynchronizationEndpoint primaryEndpoint;
    private Map<String, SpaceSynchronizationEndpoint> secondaryEndpoints;
    private String kafkaBootstrapServers;
    private Properties kafkaProperties;
    private String topic;

    public KafkaSpaceSynchronizationEndpointConfigurer primaryEndpoint(SpaceSynchronizationEndpoint synchronizationEndpoint) {
        this.primaryEndpoint = synchronizationEndpoint;
        return this;
    }

    public KafkaSpaceSynchronizationEndpointConfigurer secondaryEndpoints(Map<String, SpaceSynchronizationEndpoint> secondaryEndpoints) {
        this.secondaryEndpoints = secondaryEndpoints;
        return this;
    }

    public KafkaSpaceSynchronizationEndpointConfigurer secondaryEndpoint(String name, SpaceSynchronizationEndpoint secondaryEndpoint) {
        this.secondaryEndpoints.put(name, secondaryEndpoint);
        return this;
    }

    public KafkaSpaceSynchronizationEndpointConfigurer kafkaBootstrapServers(String kafkaBootstrapServers) {
        this.kafkaBootstrapServers = kafkaBootstrapServers;
        return this;
    }

    public KafkaSpaceSynchronizationEndpointConfigurer kafkaProperties(Properties kafkaProperties) {
        this.kafkaProperties = kafkaProperties;
        return this;
    }

    public KafkaSpaceSynchronizationEndpointConfigurer topic(String topic) {
        this.topic = topic;
        return this;
    }

    public KafkaSpaceSynchronizationEndpoint create() {
        if(kafkaProperties == null)
            kafkaProperties = new Properties();
        if(kafkaBootstrapServers != null)
            kafkaProperties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);
        return new KafkaSpaceSynchronizationEndpoint(primaryEndpoint, secondaryEndpoints, kafkaProperties, topic);
    }
}
