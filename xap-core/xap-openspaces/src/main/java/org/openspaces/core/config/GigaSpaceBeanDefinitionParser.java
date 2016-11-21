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

import org.openspaces.core.GigaSpaceConfigurer;
import org.openspaces.core.GigaSpaceFactoryBean;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.ManagedList;
import org.springframework.beans.factory.xml.AbstractSingleBeanDefinitionParser;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.core.Conventions;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;
import org.springframework.util.xml.DomUtils;
import org.w3c.dom.Attr;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;

import java.util.List;

/**
 * A bean definition builder for {@link GigaSpaceFactoryBean}.
 *
 * @author kimchy
 */
public class GigaSpaceBeanDefinitionParser extends AbstractSingleBeanDefinitionParser {

    public static final String DEFAULT_ISOLATION = "default-isolation";

    public static final String SPACE = "space";

    public static final String TX_MANAGER = "tx-manager";

    protected Class<GigaSpaceFactoryBean> getBeanClass(Element element) {
        return GigaSpaceFactoryBean.class;
    }

    @Override
    protected void doParse(Element element, ParserContext parserContext, BeanDefinitionBuilder builder) {

        handleModifierElements(element, parserContext, builder);

        NamedNodeMap attributes = element.getAttributes();
        for (int x = 0; x < attributes.getLength(); x++) {
            Attr attribute = (Attr) attributes.item(x);
            String name = attribute.getLocalName();
            if (ID_ATTRIBUTE.equals(name)) {
                continue;
            }
            String propertyName = extractPropertyName(name);
            if (DEFAULT_ISOLATION.equals(name)) {
                builder.addPropertyValue("defaultIsolationLevelName", GigaSpaceConfigurer.PREFIX_ISOLATION
                        + attribute.getValue());
                continue;
            }
            if (SPACE.equals(name)) {
                builder.addPropertyReference("space", attribute.getValue());
                continue;
            }
            if (TX_MANAGER.equals(name)) {
                builder.addPropertyReference("transactionManager", attribute.getValue());
                continue;
            }

            Assert.state(StringUtils.hasText(propertyName),
                    "Illegal property name returned from 'extractPropertyName(String)': cannot be null or empty.");
            builder.addPropertyValue(propertyName, attribute.getValue());
        }

    }

    private static enum ModifierElement {
        Write, Read, Take, Count, Clear, Change
    }

    private void handleModifierElements(Element element, ParserContext parserContext,
                                        BeanDefinitionBuilder builder) {
        handleModifierElements(element, parserContext, builder, ModifierElement.Write);
        handleModifierElements(element, parserContext, builder, ModifierElement.Read);
        handleModifierElements(element, parserContext, builder, ModifierElement.Take);
        handleModifierElements(element, parserContext, builder, ModifierElement.Count);
        handleModifierElements(element, parserContext, builder, ModifierElement.Clear);
        handleModifierElements(element, parserContext, builder, ModifierElement.Change);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private void handleModifierElements(Element element, ParserContext parserContext,
                                        BeanDefinitionBuilder builder, ModifierElement modifierElementEnum) {
        List<Element> modifiers = DomUtils.getChildElementsByTagName(element,
                modifierElementEnum.name().toLowerCase() + "-modifier");
        if (modifiers.isEmpty()) {
            return;
        }

        ManagedList managedModifiers = new ManagedList();
        for (Element modifierElement : modifiers) {
            managedModifiers.add(parserContext.getDelegate().parsePropertySubElement(
                    modifierElement,
                    builder.getRawBeanDefinition(),
                    null));
        }

        builder.addPropertyValue("default" + modifierElementEnum.name() + "Modifiers",
                managedModifiers);
    }

    protected String extractPropertyName(String attributeName) {
        return Conventions.attributeNameToPropertyName(attributeName);
    }
}