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


package org.openspaces.core.config;

import org.openspaces.core.space.mode.registry.ModeAnnotationRegistry;
import org.openspaces.core.space.mode.registry.ModeAnnotationRegistryPostProcessor;
import org.openspaces.core.space.suspend.anntations.registery.SuspendTypeAnnotationRegistry;
import org.openspaces.core.space.suspend.anntations.registery.SuspendTypeAnnotationRegistryPostProcessor;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.parsing.BeanComponentDefinition;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.beans.factory.xml.BeanDefinitionParser;
import org.springframework.beans.factory.xml.ParserContext;
import org.w3c.dom.Element;

/**
 * Registers the {@link ModeAnnotationRegistry} and {@link ModeAnnotationRegistryPostProcessor}.
 *
 * @author shaiw
 */
public class AnnotationSupportBeanDefinitionParser implements BeanDefinitionParser {

    public BeanDefinition parse(Element element, ParserContext parserContext) {

        registerBeanComponent(parserContext, ModeAnnotationRegistry.class, "internal-modeAnnotationRegistry");
        registerBeanComponent(parserContext, ModeAnnotationRegistryPostProcessor.class, "internal-modeAnnotationRegistryPostProcessor");

        registerBeanComponent(parserContext, SuspendTypeAnnotationRegistry.class, "internal-suspendTypeAnnotationRegistry");
        registerBeanComponent(parserContext, SuspendTypeAnnotationRegistryPostProcessor.class, "internal-suspendTypeAnnotationRegistryPostProcessor");

        return null;
    }

    private void registerBeanComponent(ParserContext parserContext, Class<?> beanClass, String beanName) {
        BeanDefinition bd = new RootBeanDefinition(beanClass);
        BeanComponentDefinition bcd = new BeanComponentDefinition(bd, beanName);
        parserContext.registerBeanComponent(bcd);
    }

}
