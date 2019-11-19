/*
 * Copyright (c) 2008-2019, GigaSpaces Technologies, Inc. All Rights Reserved.
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
package org.openspaces.core.properties.annotation;

import org.openspaces.core.properties.BeanLevelProperties;
import org.openspaces.core.properties.BeanLevelPropertiesContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;

/**
 * @author Niv Ingberg
 * @since 15.0
 */
@Configuration
@PropertySource("classpath:service.properties")
public class BeanLevelPropertiesBeansConfig {
    private final Logger logger = LoggerFactory.getLogger(this.getClass().getName());

    @BeanLevelPropertiesContext
    private BeanLevelProperties beanLevelProperties;

    @Bean("internal-propertySourcesPlaceholderConfigurer")
    PropertySourcesPlaceholderConfigurer propertySourcesPlaceholderConfigurer() {
        logger.info("*** propertySourcesPlaceholderConfigurer");
        PropertySourcesPlaceholderConfigurer result = new PropertySourcesPlaceholderConfigurer();
        result.setLocalOverride(true);
        if (beanLevelProperties != null)
            result.setProperties(beanLevelProperties.getContextProperties());
        return result;
    }
}
