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
package org.openspaces.persistency.space;

import com.gigaspaces.datasource.SpaceTypeSchemaAdapter;
import org.openspaces.core.GigaSpace;
import org.openspaces.core.GigaSpaceConfigurer;
import org.openspaces.core.space.UrlSpaceConfigurer;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class GigaSpaceSynchronizationEndpointConfigurer {
    private String targetSpaceName;
    private String lookupGroups;
    private String lookupLocators;
    private Map<String, SpaceTypeSchemaAdapter> spaceTypeSchemaAdapters = new HashMap<>();

    public GigaSpaceSynchronizationEndpointConfigurer spaceTypeSchemaAdapters(Collection<SpaceTypeSchemaAdapter> spaceTypeSchemaAdapters) {
        spaceTypeSchemaAdapters.forEach(this::accept);
        return this;
    }

    public GigaSpaceSynchronizationEndpointConfigurer targetSpaceName(String targetSpaceName) {
        this.targetSpaceName = targetSpaceName;
        return this;
    }

    public GigaSpaceSynchronizationEndpointConfigurer lookupGroups(String lookupGroups) {
        this.lookupGroups = lookupGroups;
        return this;
    }

    public GigaSpaceSynchronizationEndpointConfigurer lookupLocators(String lookupLocators) {
        this.lookupLocators = lookupLocators;
        return this;
    }

    public GigaSpaceSynchronizationEndpointConfigurer addSpaceTypeSchemaAdapter(SpaceTypeSchemaAdapter spaceTypeSchemaAdapter){
        accept(spaceTypeSchemaAdapter);
        return this;
    }

    public GigaSpaceSynchronizationEndpoint create() {
        if(targetSpaceName == null)
            throw new IllegalStateException("Value of target space name cannot be null");
        UrlSpaceConfigurer urlSpaceConfigurer = new UrlSpaceConfigurer("jini://*/*/" + targetSpaceName);
        if(lookupLocators != null)
            urlSpaceConfigurer.lookupLocators(lookupLocators);
        if(lookupGroups != null)
            urlSpaceConfigurer.lookupGroups(lookupGroups);
        GigaSpace targetSpace = new GigaSpaceConfigurer(urlSpaceConfigurer).create();
        return new GigaSpaceSynchronizationEndpoint(targetSpace, spaceTypeSchemaAdapters);
    }

    private void accept(SpaceTypeSchemaAdapter spaceTypeSchemaAdapter) {
        this.spaceTypeSchemaAdapters.put(spaceTypeSchemaAdapter.getTypeName(), spaceTypeSchemaAdapter);
    }
}
