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


package org.openspaces.core.properties;

import org.openspaces.core.cluster.ClusterInfo;
import org.openspaces.core.cluster.ClusterInfoAware;
import org.openspaces.core.cluster.ClusterInfoPropertyPlaceholderConfigurer;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanDefinitionStoreException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.BeanNameAware;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.BeanDefinitionVisitor;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.config.PropertyPlaceholderConfigurer;
import org.springframework.util.PropertyPlaceholderHelper;

import java.util.HashSet;
import java.util.Properties;

/**
 * An extension on top of Spring {@link PropertyPlaceholderConfigurer} that works with {@link
 * BeanLevelProperties} in order to inject bean level properties.
 *
 * <p> ${..} notations are used to lookup bean level properties with the properties obtained based
 * on the bean name using {@link BeanLevelProperties#getMergedBeanProperties(String)}.
 *
 * @author kimchy
 */
public class BeanLevelPropertyPlaceholderConfigurer extends PropertyPlaceholderConfigurer implements BeanNameAware,
        BeanFactoryAware, BeanLevelPropertiesAware, ClusterInfoAware {

    private BeanLevelProperties beanLevelProperties;

    private ClusterInfo clusterInfo;

    public BeanLevelPropertyPlaceholderConfigurer(BeanLevelProperties beanLevelProperties) {
        init(beanLevelProperties);
    }

    public BeanLevelPropertyPlaceholderConfigurer(BeanLevelProperties beanLevelProperties, ClusterInfo clusterInfo) {
        init(beanLevelProperties);
        this.clusterInfo = clusterInfo;
    }

    public void setBeanLevelProperties(BeanLevelProperties beanLevelProperties) {
        init(beanLevelProperties);
    }

    public void setClusterInfo(ClusterInfo clusterInfo) {
        this.clusterInfo = clusterInfo;
    }

    private void init(BeanLevelProperties beanLevelProperties) {
        this.beanLevelProperties = beanLevelProperties;
        setIgnoreUnresolvablePlaceholders(true);
        setSystemPropertiesMode(SYSTEM_PROPERTIES_MODE_NEVER);
        setOrder(2);
    }


    private String beanName;

    private BeanFactory beanFactory;

    public void setBeanName(String beanName) {
        this.beanName = beanName;
    }

    public void setBeanFactory(BeanFactory beanFactory) {
        this.beanFactory = beanFactory;
    }

    protected void processProperties(ConfigurableListableBeanFactory beanFactoryToProcess, Properties props)
            throws BeansException {

        String[] beanNames = beanFactoryToProcess.getBeanDefinitionNames();
        for (String beanName1 : beanNames) {
            // Check that we're not parsing our own bean definition,
            // to avoid failing on unresolvable placeholders in properties file locations.
            if (!(beanName1.equals(this.beanName) && beanFactoryToProcess.equals(this.beanFactory))) {
                Properties beanLevelProps = beanLevelProperties.getMergedBeanProperties(beanName1);
                // add cluster info properties as well
                beanLevelProps.putAll(ClusterInfoPropertyPlaceholderConfigurer.createProperties(clusterInfo));

                BeanDefinitionVisitor visitor = new PlaceholderResolvingBeanDefinitionVisitor(beanLevelProps);
                BeanDefinition bd = beanFactoryToProcess.getBeanDefinition(beanName1);
                try {
                    visitor.visitBeanDefinition(bd);
                } catch (BeanDefinitionStoreException ex) {
                    throw new BeanDefinitionStoreException(bd.getResourceDescription(), beanName1, ex.getMessage());
                }
            }
        }
    }

    /**
     * BeanDefinitionVisitor that resolves placeholders in String values, delegating to the
     * <code>parseStringValue</code> method of the containing class.
     */
    private class PlaceholderResolvingBeanDefinitionVisitor extends BeanDefinitionVisitor {

        private final Properties props;
        private final PropertyPlaceholderHelper propertyPlaceholderHelper;

        public PlaceholderResolvingBeanDefinitionVisitor(Properties props) {
            this.props = props;
            this.propertyPlaceholderHelper = new PropertyPlaceholderHelper("${", "}");
        }

        protected String resolveStringValue(String strVal) throws BeansException {
            return propertyPlaceholderHelper.replacePlaceholders(strVal, props);
        }
    }
}
