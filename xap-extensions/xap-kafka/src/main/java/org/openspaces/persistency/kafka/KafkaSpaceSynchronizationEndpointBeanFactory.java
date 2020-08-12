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