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
package com.gigaspaces.datasource;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Alon Shoham
 * @since 15.5.0
 */
public class SpaceDataSourceLoadRequest {
    private final SpaceDataSourceFactory factory;
    private final Map<String,SpaceTypeSchemaAdapter> adaptersMap = new HashMap<>();;

    public SpaceDataSourceLoadRequest(SpaceDataSourceFactory factory, Collection<SpaceTypeSchemaAdapter> adapters) {
        this.factory = factory;
        adapters.forEach(this::accept);
    }

    public SpaceDataSourceLoadRequest(SpaceDataSourceFactory factory) {
        this(factory, Collections.emptyList());
    }

    public SpaceDataSourceLoadRequest addTypeAdapter(SpaceTypeSchemaAdapter typeAdapter) {
        accept(typeAdapter);
        return this;
    }

    public SpaceDataSourceFactory getFactory() {
        return factory;
    }

    public Map<String, SpaceTypeSchemaAdapter> getAdaptersMap() {
        return adaptersMap;
    }

    private void accept(SpaceTypeSchemaAdapter typeAdapter){
        adaptersMap.put(typeAdapter.getTypeName(), typeAdapter);
    }
}
