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

import com.j_spaces.core.IJSpace;
import org.openspaces.core.GigaSpaceFactoryBean;
import org.openspaces.core.space.AbstractSpaceFactoryBean;
import org.openspaces.core.transaction.manager.DistributedJiniTransactionManager;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.transaction.PlatformTransactionManager;

import javax.annotation.Resource;

/**
 * @author Niv Ingberg
 * @since 15.0
 */
@Configuration
public abstract class AbstractSpaceBeansConfig {
    private final String spaceFactoryBeanName = "space";
    private final String gigaSpaceFactoryBeanName = "gigaSpace";
    private final String transactionManagerBeanName = "txn-manager";

    @Resource
    private ApplicationContext applicationContext;

    @Resource
    private Environment environment;

    //@Value("${space.name}")
    private String spaceName;

    @Value("${space.transactional:false}")
    private boolean transactional;

    @Bean(spaceFactoryBeanName)
    AbstractSpaceFactoryBean spaceFactoryBean() {
        return createSpaceFactoryBean();
    }

    protected abstract AbstractSpaceFactoryBean createSpaceFactoryBean();

    @Bean(gigaSpaceFactoryBeanName)
    GigaSpaceFactoryBean gigaSpaceFactoryBean() {
        GigaSpaceFactoryBean factoryBean = new GigaSpaceFactoryBean();
        configure(factoryBean);
        return factoryBean;
    }

    protected void configure(GigaSpaceFactoryBean factoryBean) {
        factoryBean.setSpace(applicationContext.getBean(spaceFactoryBeanName, IJSpace.class));
        if (transactional)
            factoryBean.setTransactionManager(applicationContext.getBean(transactionManagerBeanName, PlatformTransactionManager.class));
    }

    @Bean(transactionManagerBeanName)
    PlatformTransactionManager transactionManagerBean() {
        if (!transactional)
            return null;
        DistributedJiniTransactionManager transactionManager = new DistributedJiniTransactionManager();
        configure(transactionManager);
        return transactionManager;
    }

    protected void configure(DistributedJiniTransactionManager transactionManager) {
    }

    protected String getSpaceName() {
        if (spaceName == null) {
            spaceName = environment.containsProperty("space.name")
                    ? environment.getProperty("space.name")
                    : environment.getProperty("dataGridName");
        }
        return spaceName;
    }
}
