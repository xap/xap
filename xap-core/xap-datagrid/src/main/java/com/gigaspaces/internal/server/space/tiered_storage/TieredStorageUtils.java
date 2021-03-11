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

import com.gigaspaces.internal.server.metadata.IServerTypeDesc;
import com.gigaspaces.internal.server.space.SpaceEngine;
import com.gigaspaces.internal.server.storage.EntryTieredMetaData;
import com.gigaspaces.internal.server.storage.IEntryData;
import com.gigaspaces.internal.server.storage.IEntryHolder;
import com.j_spaces.core.cache.context.Context;
import com.j_spaces.core.cache.context.TieredState;
import com.j_spaces.core.sadapter.SAException;

import java.util.HashMap;
import java.util.Map;

public class TieredStorageUtils {
    public static Map<Object, EntryTieredMetaData> getEntriesTieredMetaDataByIds(SpaceEngine space, String typeName, Object[] ids) throws Exception {
        Map<Object, EntryTieredMetaData> entryTieredMetaDataMap =  new HashMap<>();
        if (!space.isTieredStorage()) {
            throw new Exception("Tiered storage undefined");
        }
        Context context = null;
        try{
            context = space.getCacheManager().getCacheContext();
            for (Object id : ids) {
                entryTieredMetaDataMap.put(id, getEntryTieredMetaDataById(space, typeName, id, context));
            }
        } finally{
            space.getCacheManager().freeCacheContext(context);
        }
        return entryTieredMetaDataMap;
    }

    private static EntryTieredMetaData getEntryTieredMetaDataById(SpaceEngine space, String typeName, Object id, Context context) {
        EntryTieredMetaData entryTieredMetaData = new EntryTieredMetaData();
        IServerTypeDesc typeDesc = space.getTypeManager().getServerTypeDesc(typeName);
        IEntryHolder hotEntryHolder = space.getCacheManager().getEntryByIdFromPureCache(id, typeDesc);
        IEntryHolder coldEntryHolder = null;

        try {
            coldEntryHolder = space.getTieredStorageManager().getInternalStorage().getEntry(context, typeDesc.getTypeName(), id);
        } catch (SAException e) { //entry doesn't exist in cold tier
        }

        if (hotEntryHolder != null){
            if (coldEntryHolder == null){
                entryTieredMetaData.setTieredState(TieredState.TIERED_HOT);
            } else {
                entryTieredMetaData.setTieredState(TieredState.TIERED_HOT_AND_COLD);
                entryTieredMetaData.setIdenticalToCache(isIdenticalToCache(hotEntryHolder.getEntryData(),(coldEntryHolder.getEntryData())));
            }
        } else {
            if (coldEntryHolder != null) {
                entryTieredMetaData.setTieredState(TieredState.TIERED_COLD);
            } //else- entry doesn't exist
        }
        return entryTieredMetaData;
    }

    private static boolean isIdenticalToCache(IEntryData hotEntry, IEntryData coldEntry){
        if(hotEntry.getNumOfFixedProperties() != coldEntry.getNumOfFixedProperties()){
            return false;
        }
        for(int i = 0; i < hotEntry.getNumOfFixedProperties(); ++i){
            Object hotValue = hotEntry.getFixedPropertiesValues()[i];
            Object coldValue = coldEntry.getFixedPropertiesValues()[i];
            if(hotValue == null || coldValue == null){
                return hotValue == coldValue;
            }
            if(!hotValue.equals(coldValue)){
                return false;
            }
        }
        return true;
    }
}
