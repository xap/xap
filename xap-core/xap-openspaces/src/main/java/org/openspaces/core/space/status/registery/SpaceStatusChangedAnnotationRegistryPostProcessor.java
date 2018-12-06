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


package org.openspaces.core.space.status.registery;

import org.openspaces.core.space.status.SpaceStatusChanged;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

import java.lang.reflect.Method;

/**
 * Scans the bean's methods for the annotations {@link SpaceStatusChanged} and registers them in the {@link SpaceStatusChangedAnnotationRegistry}.
 *
 * @author Elad Gur
 * @since 14.0.1
 */
public class SpaceStatusChangedAnnotationRegistryPostProcessor implements BeanPostProcessor, ApplicationContextAware {

    private ApplicationContext applicationContext;

    public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
        if (bean == null) {
            return bean;
        }
        // get the registry bean
        SpaceStatusChangedAnnotationRegistry registry = (SpaceStatusChangedAnnotationRegistry) applicationContext.getBean("internal-suspendTypeAnnotationRegistry");
        if (registry != null) {
            Class<?> beanClass = this.getBeanClass(bean);
            if (beanClass == null) {
                return bean;
            }

            // find if the bean has the relevant annotations
            for (Method method : beanClass.getMethods()) {
                if (method.isAnnotationPresent(SpaceStatusChanged.class)) {
                    registry.registerAnnotation(SpaceStatusChanged.class, bean, method);
                }
            }
        }

        return bean;
    }

    public Object postProcessBeforeInitialization(Object bean, String beanName) throws BeansException {
        return bean;
    }

    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

    private Class<?> getBeanClass(Object bean) {
        return AopUtils.getTargetClass(bean);
    }
}
