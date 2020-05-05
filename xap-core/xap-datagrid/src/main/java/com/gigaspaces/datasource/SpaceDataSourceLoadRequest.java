package com.gigaspaces.datasource;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Alon Shoham
 * @since 15.5.0
 */
public class SpaceDataSourceLoadRequest {
    private final SpaceDataSourceFactory factory;
    private final Map<String,SpaceTypeSchemaAdapter> adaptersMap = new HashMap<>();

    public SpaceDataSourceLoadRequest(SpaceDataSourceFactory factory) {
        this.factory = factory;
    }

    public SpaceDataSourceLoadRequest addTypeAdapter(SpaceTypeSchemaAdapter typeAdapter) {
        adaptersMap.put(typeAdapter.getTypeName(), typeAdapter);
        return this;
    }

    public SpaceDataSourceFactory getFactory() {
        return factory;
    }

    public Map<String, SpaceTypeSchemaAdapter> getAdaptersMap() {
        return adaptersMap;
    }
}
