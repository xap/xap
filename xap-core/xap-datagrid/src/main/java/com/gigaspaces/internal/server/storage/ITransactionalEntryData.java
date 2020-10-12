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

import com.gigaspaces.server.MutableServerEntry;
import com.j_spaces.core.SpaceOperations;
import com.j_spaces.core.XtnEntry;
import com.j_spaces.core.server.transaction.EntryXtnInfo;

import net.jini.core.transaction.server.ServerTransaction;

import java.util.Collection;
import java.util.List;

/**
 * Contains all the data (mutable) fields of the entry. when an entry is changed a new IEntryData is
 * created and attached to the IEntryHolder.
 *
 * @author Niv Ingberg
 * @since 7.0
 */
public interface ITransactionalEntryData extends IEntryData, MutableServerEntry {
    EntryXtnInfo getEntryXtnInfo();

    ITransactionalEntryData createCopyWithoutTxnInfo();

    ITransactionalEntryData createCopyWithoutTxnInfo(long newExpirationTime);

    ITransactionalEntryData createCopyWithTxnInfo(boolean createEmptyTxnInfo);

    ITransactionalEntryData createCopyWithTxnInfo(int newVersionID, long newExpirationTime);

    ITransactionalEntryData createCopyWithSuppliedTxnInfo(EntryXtnInfo ex);

    ITransactionalEntryData createCopy(boolean cloneXtnInfo, IEntryData newEntryData, long newExpirationTime);

    ITransactionalEntryData createShallowClonedCopyWithSuppliedVersion(int versionID);

    ITransactionalEntryData createShallowClonedCopyWithSuppliedVersionAndExpiration(int versionID, long expirationTime);

    default boolean anyReadLockXtn() {
        EntryXtnInfo entryXtnInfo = getEntryXtnInfo();
        return entryXtnInfo == null ? false : entryXtnInfo.anyReadLockXtn();
    }

    default List<XtnEntry> getReadLocksOwners() {
        EntryXtnInfo entryXtnInfo = getEntryXtnInfo();
        return entryXtnInfo == null ? null : entryXtnInfo.getReadLocksOwners();
    }

    default void addReadLockOwner(XtnEntry xtn) {
        getEntryXtnInfo().addReadLockOwner(xtn);
    }

    default void removeReadLockOwner(XtnEntry xtn) {
        EntryXtnInfo entryXtnInfo = getEntryXtnInfo();
        if (entryXtnInfo != null)
            entryXtnInfo.removeReadLockOwner(xtn);
    }

    default void clearReadLockOwners() {
        EntryXtnInfo entryXtnInfo = getEntryXtnInfo();
        if (entryXtnInfo != null)
            entryXtnInfo.clearReadLockOwners();
    }

    default XtnEntry getWriteLockOwner() {
        EntryXtnInfo entryXtnInfo = getEntryXtnInfo();
        return entryXtnInfo == null ? null : entryXtnInfo.getWriteLockOwner();
    }

    default void setWriteLockOwner(XtnEntry writeLockOwner) {
        EntryXtnInfo entryXtnInfo = getEntryXtnInfo();
        if (entryXtnInfo == null && writeLockOwner != null)
            throw new RuntimeException("entryTxnInfo is null");
        if (entryXtnInfo != null)
            entryXtnInfo.setWriteLockOwner(writeLockOwner);
    }

    default int getWriteLockOperation() {
        EntryXtnInfo entryXtnInfo = getEntryXtnInfo();
        return entryXtnInfo == null ? SpaceOperations.NOOP : entryXtnInfo.getWriteLockOperation();
    }

    default void setWriteLockOperation(int writeLockOperation) {
        getEntryXtnInfo().setWriteLockOperation(writeLockOperation);
    }

    default ServerTransaction getWriteLockTransaction() {
        XtnEntry owner = getWriteLockOwner();
        return owner == null ? null : owner.m_Transaction;
    }

    default IEntryHolder getOtherUpdateUnderXtnEntry() {
        EntryXtnInfo entryXtnInfo = getEntryXtnInfo();
        return entryXtnInfo == null ? null : entryXtnInfo.getOtherUpdateUnderXtnEntry();
    }

    default void setOtherUpdateUnderXtnEntry(IEntryHolder eh) {
        EntryXtnInfo entryXtnInfo = getEntryXtnInfo();
        if (entryXtnInfo == null) {
            if (eh == null)
                return;
            throw new RuntimeException("entryTxnInfo is null");
        }
        entryXtnInfo.setOtherUpdateUnderXtnEntry(eh);
    }

    default XtnEntry getXidOriginated() {
        EntryXtnInfo entryXtnInfo = getEntryXtnInfo();
        return entryXtnInfo == null ? null : entryXtnInfo.getXidOriginated();
    }

    default void setXidOriginated(XtnEntry xidOriginated) {
        EntryXtnInfo entryXtnInfo = getEntryXtnInfo();
        if (entryXtnInfo == null && xidOriginated != null)
            throw new RuntimeException("entryTxnInfo is null");
        if (entryXtnInfo != null)
            entryXtnInfo.setXidOriginated(xidOriginated);
    }

    default ServerTransaction getXidOriginatedTransaction() {
        XtnEntry originated = getXidOriginated();
        return originated == null ? null : originated.m_Transaction;
    }

    default Collection<ITemplateHolder> getWaitingFor() {
        EntryXtnInfo entryXtnInfo = getEntryXtnInfo();
        return entryXtnInfo == null ? null : entryXtnInfo.getWaitingFor();
    }

    default void initWaitingFor() {
        getEntryXtnInfo().initWaitingFor();
    }

    boolean isExpired();

    boolean isExpired(long limit);
}
