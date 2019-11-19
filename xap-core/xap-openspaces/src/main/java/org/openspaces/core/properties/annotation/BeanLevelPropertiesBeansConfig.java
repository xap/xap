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
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.Environment;
import org.springframework.core.env.PropertiesPropertySource;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;

/**
 * @author Niv Ingberg
 * @since 15.0
 */
@Configuration
@PropertySource(value = "classpath:service.properties", ignoreResourceNotFound = true)
public class BeanLevelPropertiesBeansConfig {
    private static final Logger logger = LoggerFactory.getLogger(BeanLevelPropertiesBeansConfig.class);

    @BeanLevelPropertiesContext
    private BeanLevelProperties beanLevelProperties;

    @Resource
    private Environment environment;

    @PostConstruct
    public void initialize() {
        logger.info("*** initialize");
        if (beanLevelProperties == null) {
            logger.info("beanLevelProperties is null");
        } else if (environment instanceof ConfigurableEnvironment) {
            ((ConfigurableEnvironment)environment).getPropertySources()
                    .addFirst(new PropertiesPropertySource("beanLevelProperties", beanLevelProperties.getContextProperties()));
            ((ConfigurableEnvironment) environment).getPropertySources().iterator().forEachRemaining(ps -> logger.info("propertySource: " + ps.getName()));
        }
    }

    @Bean("internal-propertySourcesPlaceholderConfigurer")
    static PropertySourcesPlaceholderConfigurer propertySourcesPlaceholderConfigurer() {
        logger.info("*** propertySourcesPlaceholderConfigurer");
        return new PropertySourcesPlaceholderConfigurer();
    }
}
