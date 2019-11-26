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

import org.openspaces.events.asyncpolling.config.AsyncPollingAnnotationPostProcessor;
import org.openspaces.events.config.AnnotationSupportBeanDefinitionParser;
import org.openspaces.events.notify.config.NotifyAnnotationPostProcessor;
import org.openspaces.events.polling.config.PollingAnnotationPostProcessor;
import org.openspaces.events.support.EventContainersBus;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Primary;

/**
 * @author Niv Ingberg
 * @since 15.0
 */
@Configuration
public class EventsAnnotationsBeansConfig {
    @Bean(AnnotationSupportBeanDefinitionParser.PRIMARY_EVENT_CONTAINER_BUS_BEAN_NAME)
    @Lazy()
    @Primary
    EventContainersBus eventContainersBus() {
        return new EventContainersBus();
    }

    @Bean("internal-pollingContainerAnnotationPostProcessor")
    PollingAnnotationPostProcessor pollingAnnotationPostProcessor() {
        return new PollingAnnotationPostProcessor();
    }

    @Bean("internal-notifyContainerAnnotationPostProcessor")
    NotifyAnnotationPostProcessor notifyAnnotationPostProcessor() {
        return new NotifyAnnotationPostProcessor();
    }

    @Bean("internal-asyncPollingContainerAnnotationPostProcessor")
    AsyncPollingAnnotationPostProcessor asyncPollingAnnotationPostProcessor() {
        return new AsyncPollingAnnotationPostProcessor();
    }
}
