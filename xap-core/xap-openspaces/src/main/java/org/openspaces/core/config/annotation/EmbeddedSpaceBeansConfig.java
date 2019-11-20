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
package org.openspaces.core.config.annotation;

import com.gigaspaces.start.ClasspathBuilder;
import org.jini.rio.boot.ServiceClassLoader;
import org.openspaces.core.space.AbstractSpaceFactoryBean;
import org.openspaces.core.space.EmbeddedSpaceFactoryBean;
import org.openspaces.core.space.EmbeddedSpaceFactoryBeanConfigurer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.env.Environment;
import org.springframework.core.env.PropertyResolver;

import javax.annotation.Resource;

/**
 * @author Niv Ingberg
 * @since 15.0
 */
public class EmbeddedSpaceBeansConfig extends AbstractSpaceBeansConfig {

    // TODO: Read from external config in $gs_home/config.
    private final CustomConfigurer[] customConfigurers = new CustomConfigurer[] {
            new CustomConfigurer("space.mx.off-heap.enabled", "memoryxtend/off-heap", "com.gigaspaces.blobstore.offheap.config.OffHeapEmbeddedSpaceFactoryBeanConfigurer"),
            new CustomConfigurer("space.mx.rocksdb.enabled", "memoryxtend/rocksdb", "com.gigaspaces.blobstore.rocksdb.config.RocksDBEmbeddedSpaceFactoryBeanConfigurer"),
    };

    @Value("${space.secured:false}")
    private boolean secured;

    @Value("${space.mirrored:false}")
    private boolean mirrored;

    @Resource
    private Environment environment;

    @Override
    protected AbstractSpaceFactoryBean createSpaceFactoryBean() {
        logger.info("*** createSpaceFactoryBean spaceName={}", getSpaceName());
        EmbeddedSpaceFactoryBean factoryBean = new EmbeddedSpaceFactoryBean();
        configure(factoryBean);
        return factoryBean;
    }

    protected void configure(EmbeddedSpaceFactoryBean factoryBean) {
        logger.info("*** configure(EmbeddedSpaceFactoryBean)");
        factoryBean.setSpaceName(getSpaceName());
        if (secured)
            factoryBean.setSecured(secured);
        if (mirrored)
            factoryBean.setMirrored(mirrored);
        for (CustomConfigurer customConfigurer : customConfigurers) {
            customConfigurer.configureIfNeeded(factoryBean, environment);
        }
    }

    private static class CustomConfigurer {
        private final String enableProperty;
        private final String optionalPath;
        private final String configurerClassName;

        private CustomConfigurer(String enableProperty, String optionalPath, String configurerClassName) {
            this.enableProperty = enableProperty;
            this.optionalPath = optionalPath;
            this.configurerClassName = configurerClassName;
        }

        public void configureIfNeeded(EmbeddedSpaceFactoryBean factoryBean, PropertyResolver propertyResolver) {
            if (propertyResolver.getProperty(enableProperty, Boolean.class, Boolean.FALSE)) {
                ServiceClassLoader.appendIfContext(() -> new ClasspathBuilder().appendOptionalJars(optionalPath));
                try {
                    EmbeddedSpaceFactoryBeanConfigurer configurer = Class.forName(configurerClassName)
                            .asSubclass(EmbeddedSpaceFactoryBeanConfigurer.class)
                            .newInstance();
                    configurer.configure(factoryBean, propertyResolver);
                } catch (ReflectiveOperationException e) {
                    throw new RuntimeException("Failed to configure space using " + configurerClassName, e);
                }
            }
        }
    }
}
