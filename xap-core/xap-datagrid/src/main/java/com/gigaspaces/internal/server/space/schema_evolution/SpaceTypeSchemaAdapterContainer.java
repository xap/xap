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
package com.gigaspaces.internal.server.space.schema_evolution;

import com.gigaspaces.datasource.SpaceTypeSchemaAdapter;
import com.gigaspaces.document.DocumentObjectConverter;
import com.gigaspaces.document.SpaceDocument;
import com.gigaspaces.metadata.SpaceTypeDescriptor;

import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author Alon Shoham
 * @since 15.5.0
 */
public class SpaceTypeSchemaAdapterContainer {
    private final Map<String, SpaceTypeSchemaAdapter> adaptersMap;

    public SpaceTypeSchemaAdapterContainer(Map<String, SpaceTypeSchemaAdapter> adaptersMap) {
        this.adaptersMap = adaptersMap;
    }

    public Object adapt(Object object) {
        if(object instanceof SpaceTypeDescriptor) {
            return adaptTypeDescriptor((SpaceTypeDescriptor) object);
        }
        SpaceDocument spaceDocument = DocumentObjectConverter.instance().toSpaceDocument(object);
        SpaceTypeSchemaAdapter spaceTypeSchemaAdapter = adaptersMap.get(spaceDocument.getTypeName());
        return spaceTypeSchemaAdapter == null ? spaceDocument : spaceTypeSchemaAdapter.adaptEntry(spaceDocument);
    }

    private SpaceTypeDescriptor adaptTypeDescriptor(SpaceTypeDescriptor spaceTypeDescriptor) {
        SpaceTypeSchemaAdapter spaceTypeSchemaAdapter = adaptersMap.get(spaceTypeDescriptor.getTypeName());
        return spaceTypeSchemaAdapter == null ? spaceTypeDescriptor : spaceTypeSchemaAdapter.adaptTypeDescriptor(spaceTypeDescriptor);
    }

    @Override
    public String toString() {
        return "SpaceTypeSchemaAdapterContainer{" +
                "adaptersMap=" + convertWithStream(adaptersMap) +
                '}';
    }

    private String convertWithStream(Map<String, SpaceTypeSchemaAdapter> map) {
        String mapAsString = map.keySet().stream()
                .map(key -> key + "=" + map.get(key))
                .collect(Collectors.joining(", ", "{", "}"));
        return mapAsString;
    }
}
