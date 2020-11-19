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
import org.openspaces.persistency.space.GigaSpaceSynchronizationEndpoint;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;

import java.util.Map;
import java.util.Properties;

public class KafkaSpaceSynchronizationEndpointBeanFactory implements FactoryBean<KafkaSpaceSynchronizationEndpoint>, InitializingBean, DisposableBean {
    private final KafkaSpaceSynchronizationEndpointConfigurer configurer = getConfigurer();
    private KafkaSpaceSynchronizationEndpoint kafkaSpaceSynchronizationEndpoint;

    protected KafkaSpaceSynchronizationEndpointConfigurer getConfigurer() {
        return new KafkaSpaceSynchronizationEndpointConfigurer();
    }

    @Override
    public KafkaSpaceSynchronizationEndpoint getObject() {
        return kafkaSpaceSynchronizationEndpoint;
    }

    @Override
    public Class<?> getObjectType() {
        return KafkaSpaceSynchronizationEndpoint.class;
    }

    @Override
    public boolean isSingleton() {
        return true;
    }

    public void setPrimaryEndpoint(SpaceSynchronizationEndpoint synchronizationEndpoint) {
        configurer.primaryEndpoint(synchronizationEndpoint);
    }

    public void setSecondaryEndpoints(Map<String,SpaceSynchronizationEndpoint> secondaryEndpoints) {
        configurer.secondaryEndpoints(secondaryEndpoints);
    }

    public void setKafkaProperties(Properties kafkaProperties){
        configurer.kafkaProperties(kafkaProperties);
    }

    public void setKafkaBootstrapServers(String kafkaBootstrapServers){
        configurer.kafkaBootstrapServers(kafkaBootstrapServers);
    }

    public void setTopic(String topic){
        configurer.topic(topic);
    }

    @Override
    public void destroy() throws Exception {
        kafkaSpaceSynchronizationEndpoint.close();
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        this.kafkaSpaceSynchronizationEndpoint = configurer.create();
    }
}