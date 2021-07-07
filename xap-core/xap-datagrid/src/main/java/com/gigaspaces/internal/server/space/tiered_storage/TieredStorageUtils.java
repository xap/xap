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

import com.gigaspaces.internal.metadata.EntryType;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.metadata.PropertyInfo;
import com.gigaspaces.internal.server.metadata.IServerTypeDesc;
import com.gigaspaces.internal.server.space.SpaceEngine;
import com.gigaspaces.internal.server.space.SpaceUidFactory;
import com.gigaspaces.internal.server.storage.*;
import com.j_spaces.core.cache.context.Context;
import com.j_spaces.core.cache.context.TemplateMatchTier;
import com.j_spaces.core.cache.context.TieredState;
import com.j_spaces.core.sadapter.SAException;
import net.jini.core.lease.Lease;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.*;

import static com.gigaspaces.internal.server.space.tiered_storage.SqliteUtils.checkEquals;
import static com.gigaspaces.internal.server.space.tiered_storage.SqliteUtils.getPropertyValue;

public class TieredStorageUtils {
    private static Logger logger = LoggerFactory.getLogger(TieredStorageUtils.class);
    private static Set<String> supportedTypes = initTypes();

    private static Set<String> initTypes() {
        Set<String> types = new HashSet<>();
        types.add(String.class.getName());
        types.add(boolean.class.getName());
        types.add(Boolean.class.getName());
        types.add(byte.class.getName());
        types.add(Byte.class.getName());
        types.add(short.class.getName());
        types.add(Short.class.getName());
        types.add(int.class.getName());
        types.add(Integer.class.getName());
        types.add(long.class.getName());
        types.add(Long.class.getName());
        types.add(BigInteger.class.getName());
        types.add(BigDecimal.class.getName());
        types.add(float.class.getName());
        types.add(Float.class.getName());
        types.add(double.class.getName());
        types.add(Double.class.getName());
        types.add(byte[].class.getName());
        types.add(Byte[].class.getName());
        types.add(Instant.class.getName());
        types.add(Timestamp.class.getName());
        types.add(Date.class.getName());
        types.add(java.sql.Date.class.getName());
        types.add(Time.class.getName());
        types.add(LocalDate.class.getName());
        types.add(LocalTime.class.getName());
        types.add(LocalDateTime.class.getName());
        return types;
    }

    public static Map<Object, EntryTieredMetaData> getEntriesTieredMetaDataByIds(SpaceEngine space, String typeName, Object[] ids) throws Exception {
        Map<Object, EntryTieredMetaData> entryTieredMetaDataMap = new HashMap<>();
        if (!space.isTieredStorage()) {
            throw new Exception("Tiered storage undefined");
        }
        Context context = null;
        try {
            context = space.getCacheManager().getCacheContext();
            for (Object id : ids) {
                entryTieredMetaDataMap.put(id, getEntryTieredMetaDataById(space, typeName, id, context));
            }
        } finally {
            space.getCacheManager().freeCacheContext(context);
        }
        return entryTieredMetaDataMap;
    }

    private static EntryTieredMetaData getEntryTieredMetaDataById(SpaceEngine space, String typeName, Object id, Context context) {
        EntryTieredMetaData entryTieredMetaData = new EntryTieredMetaData();
        IServerTypeDesc typeDesc = space.getTypeManager().getServerTypeDesc(typeName);
        IEntryHolder hotEntryHolder;

        if (typeDesc.getTypeDesc().isAutoGenerateId()) {
            hotEntryHolder = space.getCacheManager().getEntryByUidFromPureCache(((String) id));
        } else {
            hotEntryHolder = space.getCacheManager().getEntryByIdFromPureCache(id, typeDesc);
        }
        IEntryHolder coldEntryHolder = null;

        try {
            if (typeDesc.getTypeDesc().isAutoGenerateId()) {
                coldEntryHolder = space.getTieredStorageManager().getInternalStorage().getEntryByUID(context, typeDesc.getTypeName(), (String) id, null);
            } else {
                coldEntryHolder = space.getTieredStorageManager().getInternalStorage().getEntryById(context, typeDesc.getTypeName(), id, null);
            }
        } catch (SAException e) { //entry doesn't exist in cold tier
        }

        if (hotEntryHolder != null) {
            if (coldEntryHolder == null) {
                entryTieredMetaData.setTieredState(TieredState.TIERED_HOT);
            } else {
                entryTieredMetaData.setTieredState(TieredState.TIERED_HOT_AND_COLD);
                entryTieredMetaData.setIdenticalToCache(isIdenticalToCache(typeDesc.getTypeDesc(), hotEntryHolder, (coldEntryHolder)));
            }
        } else {
            if (coldEntryHolder != null) {
                entryTieredMetaData.setTieredState(TieredState.TIERED_COLD);
            } //else- entry doesn't exist
        }
        return entryTieredMetaData;
    }

    private static boolean isIdenticalToCache(ITypeDesc typeDesc, IEntryHolder hotEntryHolder, IEntryHolder coldEntryHolder) {
        IEntryData hotEntry = hotEntryHolder.getEntryData();
        IEntryData coldEntry = coldEntryHolder.getEntryData();
        if (hotEntry.getNumOfFixedProperties() != coldEntry.getNumOfFixedProperties()) {
            return false;
        }
        for (int i = 0; i < hotEntry.getNumOfFixedProperties(); ++i) {
            Object hotValue;
            Object coldValue;
            PropertyInfo property;
            if (typeDesc.isAutoGenerateId() && ((PropertyInfo) typeDesc.getFixedProperty(typeDesc.getIdPropertyName())).getOriginalIndex() == i) {
                property = (PropertyInfo) typeDesc.getFixedProperty(typeDesc.getIdPropertyName());
                hotValue = hotEntryHolder.getUID();
                coldValue = coldEntryHolder.getUID();
            } else {
                property = typeDesc.getFixedProperty(i);
                hotValue = hotEntry.getFixedPropertiesValues()[i];
                coldValue = coldEntry.getFixedPropertiesValues()[i];
            }

            if (hotValue == null || coldValue == null) {
                if(hotValue != coldValue){
                    return false;
                }else {
                    continue;
                }
            }

            if (!checkEquals(property.getType(),hotValue,coldValue)) {
                logger.warn("Failed to have consistency between hot and cold tier for id: " +
                        hotEntry.getEntryDataType().name() + " Hot: " + hotValue + " Cold: " + coldValue);
                return false;
            }
        }
        return true;
    }

    public static List<String> getTiersAsList(TemplateMatchTier templateTieredState) {
        switch (templateTieredState) {
            case MATCH_HOT:
                return Collections.singletonList("HOT");
            case MATCH_COLD:
                return Collections.singletonList("COLD");
            case MATCH_HOT_AND_COLD:
                return Arrays.asList("HOT", "COLD");
        }

        throw new IllegalStateException("Should be unreachable");
    }

    public static IEntryHolder getEntryHolderFromRow(IServerTypeDesc serverTypeDesc, ResultSet resultSet) throws SQLException {
        ITypeDesc typeDesc = serverTypeDesc.getTypeDesc();
        PropertyInfo[] properties = typeDesc.getProperties();
        Object[] values = new Object[properties.length];
        for (int i = 0; i < properties.length; i++) {
            values[i] = getPropertyValue(resultSet, properties[i].getType(), properties[i].getOriginalIndex() + 1);
        }
        FlatEntryData data = new FlatEntryData(values, null, typeDesc.getEntryTypeDesc(EntryType.DOCUMENT_JAVA), 0, Lease.FOREVER, null);
        String uid;
        if (typeDesc.isAutoGenerateId()) {
            uid = (String) data.getFixedPropertyValue(((PropertyInfo) typeDesc.getFixedProperty(typeDesc.getIdPropertyName())).getOriginalIndex());
        } else {
            Object idFromEntry = data.getFixedPropertyValue(((PropertyInfo) typeDesc.getFixedProperty(typeDesc.getIdPropertyName())).getOriginalIndex());
            uid = SpaceUidFactory.createUidFromTypeAndId(typeDesc, idFromEntry);
        }
        return new EntryHolder(serverTypeDesc, uid, 0, false, data);
    }

    public static boolean isSupportedPropertyType(Class<?> type) {
        return supportedTypes.contains(type.getName());
    }

    public static boolean isSupportedTimeColumn(Class<?> type) {
        return type.equals(Instant.class) || type.equals(Timestamp.class) || type.equals(long.class) || type.equals(Long.class)
                || type.equals(Date.class) || type.equals(LocalDateTime.class);
    }
}
