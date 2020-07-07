package org.openspaces.persistency.space;

import com.gigaspaces.datasource.SpaceTypeSchemaAdapter;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;

import java.util.Collection;

public class GigaSpaceSynchronizationEndpointBeanFactory implements FactoryBean<GigaSpaceSynchronizationEndpoint>, InitializingBean{
    private GigaSpaceSynchronizationEndpointConfigurer configurer = getConfigurer();
    private GigaSpaceSynchronizationEndpoint gigaSpaceSynchronizationEndpoint;

    protected GigaSpaceSynchronizationEndpointConfigurer getConfigurer() {
        return new GigaSpaceSynchronizationEndpointConfigurer();
    }

    public void setSpaceTypeSchemaAdapters(Collection<SpaceTypeSchemaAdapter> spaceTypeSchemaAdapters){
        configurer.spaceTypeSchemaAdapters(spaceTypeSchemaAdapters);
    }

    public void setTargetSpaceName(String targetSpaceName){
        configurer.targetSpaceName(targetSpaceName);
    }

    public void setLookupGroups(String lookupGroups){
        configurer.lookupGroups(lookupGroups);
    }

    public void setLookupLocators(String lookupLocators){
        configurer.lookupLocators(lookupLocators);
    }

    @Override
    public GigaSpaceSynchronizationEndpoint getObject() {
        return gigaSpaceSynchronizationEndpoint;
    }

    @Override
    public Class<?> getObjectType() {
        return GigaSpaceSynchronizationEndpoint.class;
    }

    @Override
    public boolean isSingleton() {
        return true;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        this.gigaSpaceSynchronizationEndpoint = configurer.create();
    }
}
