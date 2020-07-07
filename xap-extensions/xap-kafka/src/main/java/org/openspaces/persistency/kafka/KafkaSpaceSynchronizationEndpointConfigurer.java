package org.openspaces.persistency.kafka;

import com.gigaspaces.sync.SpaceSynchronizationEndpoint;
import org.apache.kafka.clients.CommonClientConfigs;

import java.util.Properties;

public class KafkaSpaceSynchronizationEndpointConfigurer {
    private SpaceSynchronizationEndpoint spaceSynchronizationEndpoint;
    private String kafkaBootstrapServers;
    private Properties kafkaProperties;
    private String topic;

    public KafkaSpaceSynchronizationEndpointConfigurer spaceSynchronizationEndpoint(SpaceSynchronizationEndpoint synchronizationEndpoint) {
        this.spaceSynchronizationEndpoint = synchronizationEndpoint;
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
        KafkaSpaceSynchronizationEndpoint kafkaSpaceSynchronizationEndpoint = new KafkaSpaceSynchronizationEndpoint(spaceSynchronizationEndpoint, kafkaProperties, topic);
        kafkaSpaceSynchronizationEndpoint.start();
        return kafkaSpaceSynchronizationEndpoint;
    }
}

