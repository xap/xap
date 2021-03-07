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
package org.mule.config.spring;

import org.mule.api.config.ConfigurationException;
import org.mule.config.ConfigResource;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class GSSpringXmlConfigurationBuilder extends SpringXmlConfigurationBuilder {

    public static final String GS_MULE_DEFAULTS_CONFIG = "default-mule-config-gs.xml";
    public static final String GS_MULE_SPRING_CONFIG = "mule-spring-config-gs.xml";
    public static final String GS_MULE_MINIMAL_SPRING_CONFIG = "minimal-mule-config-gs.xml";
    public static final String GS_MULE_REGISTRY_BOOTSTRAP_SPRING_CONFIG = "registry-bootstrap-mule-config-gs.xml";
    public static final String GS_MULE_DOMAIN_REGISTRY_BOOTSTRAP_SPRING_CONFIG = "registry-bootstrap-mule-domain-config-gs.xml";

    public GSSpringXmlConfigurationBuilder(String configResources) throws ConfigurationException {
        super(configResources);
    }


    @Override
    protected void addResources(List<ConfigResource> allResources) {
        allResources.clear();
        try {

            if (useMinimalConfigResource)
            {
                allResources.add(new ConfigResource(GS_MULE_DOMAIN_REGISTRY_BOOTSTRAP_SPRING_CONFIG));
                allResources.add(new ConfigResource(GS_MULE_MINIMAL_SPRING_CONFIG));
                allResources.add(new ConfigResource(GS_MULE_SPRING_CONFIG));
                allResources.addAll(Arrays.asList(configResources));
            }
            else if (useDefaultConfigResource)
            {
                allResources.add(new ConfigResource(GS_MULE_REGISTRY_BOOTSTRAP_SPRING_CONFIG));
                allResources.add(new ConfigResource(GS_MULE_MINIMAL_SPRING_CONFIG));
                allResources.add(new ConfigResource(GS_MULE_SPRING_CONFIG));
                allResources.add( new ConfigResource(GS_MULE_DEFAULTS_CONFIG));
                allResources.addAll(Arrays.asList(configResources));
            }
            else
            {
                allResources.add(new ConfigResource(GS_MULE_SPRING_CONFIG));
                allResources.addAll(Arrays.asList(configResources));
            }
        } catch (IOException e) {
            logger.error("Failed to add resources", e);
            throw new RuntimeException(e);
        }
    }
}
