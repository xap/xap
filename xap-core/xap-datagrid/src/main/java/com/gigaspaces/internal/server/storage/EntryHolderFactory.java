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

import com.gigaspaces.client.storage_adapters.class_storage_adapters.ClassBinaryStorageAdapter;
import com.gigaspaces.internal.metadata.EntryType;
import com.gigaspaces.internal.metadata.EntryTypeDesc;
import com.gigaspaces.internal.server.metadata.IServerTypeDesc;
import com.gigaspaces.internal.server.space.SpaceUidFactory;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.time.SystemTime;
import com.j_spaces.core.LeaseManager;
import com.j_spaces.core.XtnEntry;
import com.j_spaces.core.cache.EntryCacheInfoFactory;
import com.j_spaces.core.cache.IEntryCacheInfo;
import com.j_spaces.core.cache.blobStore.BlobStoreEntryHolder;
import com.j_spaces.core.server.transaction.EntryXtnInfo;
import com.j_spaces.kernel.IObjectInfo;
import com.j_spaces.kernel.IStoredList;

import java.util.ArrayList;

/**
 * Factory to create EntryHolder instances.
 *
 * @author Niv Ingberg
 * @since 7.0
 */
@com.gigaspaces.api.InternalApi
public class EntryHolderFactory {
    protected EntryHolderFactory() {
    }

    public static IEntryHolder createEntryHolder(IServerTypeDesc typeDesc, IEntryPacket entryPacket,
                                                 EntryDataType entryDataType, String uid, long expirationTime, XtnEntry xidOriginated, long scn, boolean blobStoreEntryHolder) {
        return createEntryHolder(typeDesc, entryPacket,
                entryDataType, uid, expirationTime, xidOriginated, scn,
                -1 /*versionID*/, false /*keepExpiration*/, blobStoreEntryHolder);
    }


    public static IEntryHolder createEntryHolder(IServerTypeDesc typeDesc, IEntryPacket entryPacket,
                                                 EntryDataType entryDataType, String uid, long expirationTime,
                                                 XtnEntry xidOriginated, long scn, int versionID, boolean keepExpiration, boolean blobStoreEntryHolder) {
        if (blobStoreEntryHolder)
            return
                    createBlobStoreEntryHolder(typeDesc, entryPacket,
                            entryDataType, uid, expirationTime,
                            xidOriginated, scn, versionID, keepExpiration);

        ITransactionalEntryData entryData =
                createEntryData(entryPacket, entryDataType, versionID,
                        expirationTime, xidOriginated != null, keepExpiration);

        if (xidOriginated != null)
            entryData.setXidOriginated(xidOriginated);

        return new EntryHolder(typeDesc, uid, scn, entryPacket.isTransient(), entryData);
    }

    /**
     * Used by DataAdaptor, the EntryHolder Ctor make sure the version is always bigger than 0.
     */
    public static IEntryHolder createEntryHolder(IServerTypeDesc typeDesc,
                                                 IEntryPacket entryPacket, EntryDataType entryDataType) {
        String uid;
        if (entryPacket.getUID() != null)
            uid = entryPacket.getUID();
        else //generate UID from Primary key
            uid = SpaceUidFactory.createUidFromTypeAndId(typeDesc.getTypeDesc(),
                    entryPacket.getPropertyValue(typeDesc.getTypeDesc().getDefaultPropertyName()));

        int version = entryPacket.getVersion();
        ITransactionalEntryData entryData = createEntryData(entryPacket, entryDataType,
                version > 0 ? version : 1, -1 /*lease*/, false /*createEntryXtnInfo*/, false /*keepExpiration*/);

        return new EntryHolder(typeDesc, uid, SystemTime.timeMillis(), entryPacket.isTransient(), entryData);
    }

    public static IEntryHolder createEntryHolder(IServerTypeDesc typeDesc,
                                                 ITransactionalEntryData entryData, String uid, boolean isTransient) {
        return new EntryHolder(typeDesc, uid, SystemTime.timeMillis(), isTransient, entryData);

    }

    public static ShadowEntryHolder createShadowEntryHolder(IEntryHolder master,
                                                            ArrayList<IObjectInfo<IEntryCacheInfo>> backRefs, int[] backrefIndexPos,
                                                            IStoredList<Object> leaseManagerListRef, IObjectInfo<Object> leaseManagerPosRef) {
        return new ShadowEntryHolder(master, backRefs, backrefIndexPos, leaseManagerListRef, leaseManagerPosRef);
    }


    private static IEntryHolder createBlobStoreEntryHolder(IServerTypeDesc typeDesc, IEntryPacket entryPacket,
                                                           EntryDataType entryDataType, String uid, long expirationTime,
                                                           XtnEntry xidOriginated, long scn, int versionID, boolean keepExpiration) {
        ITransactionalEntryData entryData =
                createEntryData(entryPacket, entryDataType, versionID,
                        expirationTime, xidOriginated != null, keepExpiration);

        if (xidOriginated != null)
            entryData.setXidOriginated(xidOriginated);

        IEntryHolder entryHolder = new BlobStoreEntryHolder(typeDesc, uid, scn, entryPacket.isTransient(), entryData,false/*optimizedEntry*/);
        EntryCacheInfoFactory.createBlobStoreEntryCacheInfo(entryHolder);//create the cache info as a resident part for locking etc
        return entryHolder;
    }

    private static ITransactionalEntryData createEntryData(IEntryPacket entryPacket,
                                                           EntryDataType entryDataType, int versionID, long expiration, boolean createXtnEntryInfo, boolean keepExpiration) {
        final EntryType entryType = entryPacket.getEntryType();
        if (entryType == null)
            throw new IllegalStateException("entryPacket.getEntryType() is null (packet class: " + entryPacket.getClass().getName() + ").");

        final EntryTypeDesc entryTypeDesc = entryPacket.getTypeDescriptor().getEntryTypeDesc(entryType);
        final int version = versionID > 0 ? versionID : entryPacket.getVersion();
        final long lease = (expiration > 0 || keepExpiration) ? expiration : LeaseManager.toAbsoluteTime(entryPacket.getTTL());
        final EntryXtnInfo entryXtnInfo = createXtnEntryInfo ? new EntryXtnInfo() : null;

        if (entryDataType == EntryDataType.FLAT) {
            ClassBinaryStorageAdapter adapter = (entryTypeDesc.getTypeDesc()).getClassBinaryStorageAdapter();
            if (adapter != null) {
                return new BinaryEntryData(entryPacket.getFieldValues(),entryPacket.getDynamicProperties(),
                        entryTypeDesc, version, lease, entryXtnInfo);
            } else {
                return new FlatEntryData(entryPacket.getFieldValues(), entryPacket.getDynamicProperties(),
                        entryTypeDesc, version, lease, entryXtnInfo);
            }
        }

        return new UserTypeEntryData(entryPacket.toObject(entryType), entryTypeDesc, version, lease, entryXtnInfo);
    }
}
