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

import org.openspaces.core.GigaMapFactoryBean;
import org.openspaces.core.GigaSpaceConfigurer;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.xml.AbstractSingleBeanDefinitionParser;
import org.springframework.core.Conventions;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;
import org.w3c.dom.Attr;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;

/**
 * A bean definition builder for {@link org.openspaces.core.GigaMapFactoryBean}.
 *
 * @author kimchy
 */
public class GigaMapBeanDefinitionParser extends AbstractSingleBeanDefinitionParser {

    public static final String DEFAULT_ISOLATION = "default-isolation";

    public static final String MAP = "map";

    public static final String TX_MANAGER = "tx-manager";

    protected Class<GigaMapFactoryBean> getBeanClass(Element element) {
        return GigaMapFactoryBean.class;
    }

    protected void doParse(Element element, BeanDefinitionBuilder builder) {
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
            if (MAP.equals(name)) {
                builder.addPropertyReference("map", attribute.getValue());
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

    protected String extractPropertyName(String attributeName) {
        return Conventions.attributeNameToPropertyName(attributeName);
    }
}