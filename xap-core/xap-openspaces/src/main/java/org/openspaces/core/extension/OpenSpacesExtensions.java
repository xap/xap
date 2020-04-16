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

package org.openspaces.core.extension;

import com.gigaspaces.internal.extension.XapExtensionActivator;

import org.springframework.beans.factory.xml.BeanDefinitionParser;

import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author kobi on 16/05/16.
 * @since 12.0
 */
public class OpenSpacesExtensions {

    private static final Logger logger = LoggerFactory.getLogger(OpenSpacesExtensions.class.getName());
    private final Map<String, BeanDefinitionParser> coreBeanDefinitionParsers = new HashMap<String, BeanDefinitionParser>();

    private static OpenSpacesExtensions instance;

    public static synchronized OpenSpacesExtensions getInstance() {
        if (instance == null) {
            instance = new OpenSpacesExtensions();
            XapExtensionActivator.scanAndActivate(
                    OpenSpacesExtensions.class.getClassLoader(), "extensions-openspaces");
        }
        return instance;
    }

    private OpenSpacesExtensions() {
    }

    public Map<String, BeanDefinitionParser> getCoreBeanDefinitionParsers() {
        return coreBeanDefinitionParsers;
    }

    public void register(String name, BeanDefinitionParser parser) {
        coreBeanDefinitionParsers.put(name, parser);
    }
}
