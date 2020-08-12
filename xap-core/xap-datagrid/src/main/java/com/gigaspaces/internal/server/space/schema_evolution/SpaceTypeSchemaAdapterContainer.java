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
