/*
 * Copyright (c) 2008-2016, GigaSpaces Technologies, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
