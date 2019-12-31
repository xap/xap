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
import org.openspaces.pu.container.support.BeanLevelPropertiesParser;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.Environment;
import org.springframework.core.env.MutablePropertySources;
import org.springframework.core.env.PropertiesPropertySource;
import org.springframework.core.io.ClassPathResource;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.io.IOException;
import java.io.UncheckedIOException;

/**
 * @author Niv Ingberg
 * @since 15.0
 */
@Configuration
public class BeanLevelPropertiesBeansConfig {

    @BeanLevelPropertiesContext
    private BeanLevelProperties beanLevelProperties;

    @Resource
    private Environment environment;

    @PostConstruct
    public void initialize() {
        if (beanLevelProperties != null && environment instanceof ConfigurableEnvironment) {
            MutablePropertySources propertySources = ((ConfigurableEnvironment) environment).getPropertySources();
            propertySources.addFirst(new PropertiesPropertySource("beanLevelProperties", beanLevelProperties.getContextProperties()));
            ClassPathResource resource = findExistingResource("gs-service-config.yaml", "service.properties");
            if (resource != null) {
                try {
                    propertySources.addLast(new PropertiesPropertySource(resource.getFilename(), BeanLevelPropertiesParser.loadProperties(resource)));
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
        }
    }

    private static ClassPathResource findExistingResource(String ... paths) {
        for (String path : paths) {
            ClassPathResource resource = new ClassPathResource(path);
            if (resource.exists())
                return resource;
        }
        return null;
    }

    @Bean("internal-propertySourcesPlaceholderConfigurer")
    static PropertySourcesPlaceholderConfigurer propertySourcesPlaceholderConfigurer() {
        return new PropertySourcesPlaceholderConfigurer();
    }
}
