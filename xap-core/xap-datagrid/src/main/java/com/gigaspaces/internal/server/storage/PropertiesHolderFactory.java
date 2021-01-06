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
package com.gigaspaces.internal.server.storage;

import com.gigaspaces.internal.metadata.ITypeDesc;

public class PropertiesHolderFactory {

    public static PropertiesHolder create(ITypeDesc typeDesc, IEntryData entryData){
        if(entryData.isHybrid()){
            return new HybridPropertiesHolder(entryData.getEntryTypeDesc().getTypeDesc(),
                    ((HybridEntryData) entryData).getNonSerializedProperties(), ((HybridEntryData)entryData).getPackedSerializedProperties());
        } else {
            return new FlatPropertiesHolder(entryData.getFixedPropertiesValues());
        }
    }

    public static PropertiesHolder create(ITypeDesc typeDesc, Object[] fields){
        if(typeDesc.getClassBinaryStorageAdapter() != null){
            return new HybridPropertiesHolder(typeDesc, fields);
        } else {
            return new FlatPropertiesHolder(fields);
        }
    }

    public static PropertiesHolder create(){
        return new FlatPropertiesHolder();
    }

}
