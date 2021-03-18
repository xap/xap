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
package com.gigaspaces.internal.server.space.tiered_storage;

import com.gigaspaces.server.SpaceCustomComponent;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashMap;
import java.util.Map;

public class TieredStorageConfig extends SpaceCustomComponent implements Externalizable {


    private Map<String, TieredStorageTableConfig> tables;

    @Override
    public String getSpaceComponentKey() {
        return "TieredStorage";
    }

    @Override
    public String getServiceDetailAttributeName() {
        return "TieredStorage";
    }

    @Override
    public Object getServiceDetailAttributeValue() {
        return true;
    }

    public Map<String, TieredStorageTableConfig> getTables() {
        return tables;
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
}
