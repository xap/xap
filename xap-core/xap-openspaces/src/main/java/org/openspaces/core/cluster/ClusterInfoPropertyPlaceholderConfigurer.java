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


package org.openspaces.core.cluster;

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

import java.util.Properties;

/**
 * Provides the {@link org.openspaces.core.cluster.ClusterInfo ClusterInfo} parameters as injected
 * properties that can be used within Spring application context. The following is a list of the
 * properties: <ul> <li>${clusterInfo.numberOfInstances} : Maps to {@link
 * ClusterInfo#getNumberOfInstances()}</li> <li>${clusterInfo.numberOfBackups} : Maps to {@link
 * ClusterInfo#getNumberOfBackups()}</li> <li>${clusterInfo.instanceId} : Maps to {@link
 * ClusterInfo#getInstanceId()}</li> <li>${clusterInfo.backupId} : Maps to {@link
 * ClusterInfo#getBackupId()}</li> <li>${clusterInfo.schema} : Maps to {@link
 * ClusterInfo#getSchema()}</li> <li>${clusterInfo.runningNumber} : Maps to {@link
 * ClusterInfo#getRunningNumber()}</li> <li>${clusterInfo.suffix} : Maps to {@link
 * ClusterInfo#getSuffix()}</li> <li>${clusterInfo.name} : Maps to {@link
 * ClusterInfo#getName()}</li> </ul>
 *
 * <p>If the cluster info parameter is not set (has <code>null</code> value) an empty string will be
 * used.
 *
 * @author kimchy
 */
public class ClusterInfoPropertyPlaceholderConfigurer extends PropertyPlaceholderConfigurer implements BeanNameAware,
        BeanFactoryAware, ClusterInfoAware {

    public static final String NUMBER_OF_INSTANCES_PROP = "clusterInfo.numberOfInstances";
    public static final String NUMBER_OF_BACKUPS_PROP = "clusterInfo.numberOfBackups";
    public static final String INSTANCE_ID_PROP = "clusterInfo.instanceId";
    public static final String BACKUP_ID_PROP = "clusterInfo.backupId";
    public static final String SCHEMA_PROP = "clusterInfo.schema";
    public static final String RUNNING_NUMBER_PROP = "clusterInfo.runningNumber";
    public static final String RUNNING_NUMBER_OFFEST_1_PROP = "clusterInfo.runningNumberOffset1";
    public static final String SUFFIX_PROP = "clusterInfo.suffix";
    public static final String NAME_PROP = "clusterInfo.name";

    public static Properties createProperties(ClusterInfo clusterInfo) {
        Properties properties = new Properties();
        if (clusterInfo != null) {
            properties.setProperty(NUMBER_OF_INSTANCES_PROP, toPropertyValue(clusterInfo.getNumberOfInstances()));
            properties.setProperty(NUMBER_OF_BACKUPS_PROP, toPropertyValue(clusterInfo.getNumberOfBackups()));
            properties.setProperty(INSTANCE_ID_PROP, toPropertyValue(clusterInfo.getInstanceId()));
            properties.setProperty(BACKUP_ID_PROP, toPropertyValue(clusterInfo.getBackupId()));
            properties.setProperty(SCHEMA_PROP, toPropertyValue(clusterInfo.getSchema()));
            properties.setProperty(RUNNING_NUMBER_PROP, toPropertyValue(clusterInfo.getRunningNumber()));
            properties.setProperty(RUNNING_NUMBER_OFFEST_1_PROP, toPropertyValue(clusterInfo.getRunningNumberOffset1()));
            properties.setProperty(SUFFIX_PROP, toPropertyValue(clusterInfo.getSuffix()));
            properties.setProperty(NAME_PROP, toPropertyValue(clusterInfo.getName()));
        } else {
            properties.setProperty(NUMBER_OF_INSTANCES_PROP, "");
            properties.setProperty(NUMBER_OF_BACKUPS_PROP, "");
            properties.setProperty(INSTANCE_ID_PROP, "");
            properties.setProperty(BACKUP_ID_PROP, "");
            properties.setProperty(SCHEMA_PROP, "");
            properties.setProperty(RUNNING_NUMBER_PROP, "");
            properties.setProperty(RUNNING_NUMBER_OFFEST_1_PROP, "");
            properties.setProperty(SUFFIX_PROP, "");
            properties.setProperty(NAME_PROP, "");
        }
        return properties;
    }

    private static String toPropertyValue(Object value) {
        if (value == null) {
            return "";
        }
        return value.toString();
    }


    private Properties properties;

    public ClusterInfoPropertyPlaceholderConfigurer() {
    }

    public ClusterInfoPropertyPlaceholderConfigurer(ClusterInfo clusterInfo) {
        init(clusterInfo);
    }

    public void setClusterInfo(ClusterInfo clusterInfo) {
        init(clusterInfo);
    }

    private void init(ClusterInfo clusterInfo) {
        properties = createProperties(clusterInfo);
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
                BeanDefinitionVisitor visitor = new PlaceholderResolvingBeanDefinitionVisitor(properties);
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
     * BeanDefinitionVisitor that resolves placeholders in String values, delegating to
     * an instance of {@link PropertyPlaceholderHelper}.
     */
    private class PlaceholderResolvingBeanDefinitionVisitor extends BeanDefinitionVisitor {

        private final Properties props;
        private final PropertyPlaceholderHelper propertyPlaceholderHelper;

        public PlaceholderResolvingBeanDefinitionVisitor(Properties props) {
            this.props = props;
            this.propertyPlaceholderHelper = new PropertyPlaceholderHelper(DEFAULT_PLACEHOLDER_PREFIX, DEFAULT_PLACEHOLDER_SUFFIX, DEFAULT_VALUE_SEPARATOR, true);
        }

        protected String resolveStringValue(String strVal) throws BeansException {
            return propertyPlaceholderHelper.replacePlaceholders(strVal, props);
        }
    }
}