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
