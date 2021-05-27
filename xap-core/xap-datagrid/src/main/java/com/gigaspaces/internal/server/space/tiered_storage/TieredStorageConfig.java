package com.gigaspaces.internal.server.space.tiered_storage;

import com.gigaspaces.server.SpaceCustomComponent;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashMap;
import java.util.Map;

import static com.j_spaces.core.Constants.TieredStorage.SPACE_CLUSTER_INFO_TIERED_STORAGE_COMPONENT_NAME;

public class TieredStorageConfig extends SpaceCustomComponent implements Externalizable {


    private Map<String, TieredStorageTableConfig> tables;

    @Override
    public String getSpaceComponentKey() {
        return SPACE_CLUSTER_INFO_TIERED_STORAGE_COMPONENT_NAME;
    }

    @Override
    public String getServiceDetailAttributeName() {
        return SPACE_CLUSTER_INFO_TIERED_STORAGE_COMPONENT_NAME;
    }

    @Override
    public Object getServiceDetailAttributeValue() {
        return true;
    }

    public Map<String, TieredStorageTableConfig> getTables() {
        return tables;
    }

    public boolean hasCacheRule(String type){
        return getTables().get(type) != null;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        if (tables != null) {
            out.writeInt(tables.size());
            for (TieredStorageTableConfig table : tables.values()) {
                out.writeObject(table);
            }
        } else {
            out.writeInt(-1);
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        int size = in.readInt();
        if (size != -1) {
            this.tables = new HashMap<>(size);
            for (int i = 0; i < size; i++) {
                TieredStorageTableConfig tableConfig = (TieredStorageTableConfig) in.readObject();
                this.tables.put(tableConfig.getName(), tableConfig);
            }
        }

    }

    public SpaceCustomComponent setTables(Map<String, TieredStorageTableConfig> tables) {
        this.tables = tables;
        return this;
    }


    public Map<String, TieredStorageTableConfig> toMapConfigurationForDisplay(){
        HashMap<String, TieredStorageTableConfig> objectObjectHashMap = new HashMap<>();
        for (Map.Entry<String, TieredStorageTableConfig> stringTieredStorageTableConfigEntry : tables.entrySet()) {
            objectObjectHashMap.put(stringTieredStorageTableConfigEntry.getKey(), stringTieredStorageTableConfigEntry.getValue());
        }
        return  objectObjectHashMap;
    }

}
