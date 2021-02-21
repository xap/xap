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

import com.j_spaces.core.XtnEntry;

import net.jini.core.transaction.server.ServerTransaction;

import java.rmi.MarshalledObject;
import java.util.Collection;
import java.util.List;

/**
 * @author Niv Ingberg
 * @since 7.0
 */
public interface IEntryHolder extends ISpaceItem {
    void setSCN(long scn);

    int getOrder();

    void setOrder(int order);

    void setUID(String uid);

    ITransactionalEntryData getTxnEntryData();

    void updateEntryData(IEntryData newEntryData, long expirationTime);

    void resetEntryXtnInfo();

    boolean anyReadLockXtn();

    List<XtnEntry> getReadLockOwners();

    void addReadLockOwner(XtnEntry xtn);

    void removeReadLockOwner(XtnEntry xtn);

    void clearReadLockOwners();

    XtnEntry getWriteLockOwner();

    boolean isEntryUnderWriteLockXtn();

    void resetWriteLockOwner();

    int getWriteLockOperation();

    void setWriteLockOperation(int writeLockOperation, boolean createSnapshot);

    void setWriteLockOwnerAndOperation(XtnEntry writeLockOwner, int writeLockOperation);

    void setWriteLockOwnerAndOperation(XtnEntry writeLockOwner, int writeLockOperation, boolean createSnapshot);

    void setWriteLockOwnerOperationAndShadow(XtnEntry writeLockOwner, int writeLockOperation, IEntryHolder otherEh);

    ServerTransaction getWriteLockTransaction();

    void resetXidOriginated();

    boolean isShadow();

    boolean hasShadow();

    boolean hasShadow(boolean safeEntry);

    ShadowEntryHolder getShadow();

    IEntryHolder createCopy();

    IEntryHolder createDummy();

    void restoreUpdateXtnRollback(IEntryData entryData);

    void setOtherUpdateUnderXtnEntry(IEntryHolder eh);

    MarshalledObject getHandback();

    int getNotifyType();

    Collection<ITemplateHolder> getTemplatesWaitingForEntry();

    Collection<ITemplateHolder> getCopyOfTemplatesWaitingForEntry();

    void addTemplateWaitingForEntry(ITemplateHolder template);

    void removeTemplateWaitingForEntry(ITemplateHolder template);


    IEntryHolder getMaster();

    boolean isUnstable();

    void setunStable(boolean value);

    boolean isExpired();

    boolean isExpired(long limit);


    Object getRoutingValue();

    Object getEntryId();

    boolean isSameEntryInstance(IEntryHolder other);

    boolean isBlobStoreEntry();

    IEntryHolder getOriginalEntryHolder();

    void setTransient(boolean isTransient);

    default boolean isHollowEntry(){
        return false;
    }

}
