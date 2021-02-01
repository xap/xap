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
import com.gigaspaces.internal.server.metadata.IServerTypeDesc;
import com.gigaspaces.internal.server.space.SpaceUidFactory;
import com.gigaspaces.internal.utils.Textualizable;
import com.gigaspaces.internal.utils.Textualizer;
import com.j_spaces.core.client.NotifyModifiers;
import com.j_spaces.kernel.locks.IEvictableLockObject;

import java.rmi.MarshalledObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Niv Ingberg
 * @since 7.0
 */
public abstract class AbstractSpaceItem implements ISpaceItem, Textualizable {
    private final transient IServerTypeDesc _typeDesc;

    /**
     * Store Entry Unique ID. If this field is not <b>null</b> then this UID will be used by the
     * Space, otherwise the space will generate it automatically. When entry have all its fields
     * null (null template) and its UID is assigned, matching will be done using the UID only.
     *
     * The UID is a String based identifier and composed of the following parts: - Class information
     * class Hashcode and name size - Space node name At clustered environment combined from
     * container-name :space name. At non-clustered environment combined from dummy name. -
     * Timestamp - Counter
     */
    private String _uid;
    /**
     * inserting timestamp.
     */
    private long _scn;
    /**
     * A transient entry in a persistent space resides in memory and is not written to the DB.
     */
    private boolean _transient;
    //is entry deleted?  
    private transient volatile boolean _deleted;

    private transient volatile boolean _maybeUnderXtn;

    /**
     * inserting order within same time stamp.
     */
    private int _order;

    protected AbstractSpaceItem(IServerTypeDesc typeDesc, String uid,
                                long scn, boolean isTransient) {
        this._typeDesc = typeDesc;
        this._uid = uid;
        this._scn = scn;
        this._transient = isTransient;
    }

    protected AbstractSpaceItem(IEntryHolder other) {
        this(other.getServerTypeDesc(), other.getUID(), other.getSCN(), other.isTransient());
    }


    public IServerTypeDesc getServerTypeDesc() {
        return _typeDesc;
    }

    public String getClassName() {
        return _typeDesc.getTypeName();
    }

    public String getUID() {
        return _uid;
    }

    public void setUID(String uid) {
        this._uid = uid;
    }

    public long getSCN() {
        return _scn;
    }

    public void setSCN(long scn) {
        this._scn = scn;
    }

    public long getExpirationTime() {
        return getEntryData().getExpirationTime();
    }

    public abstract void setExpirationTime(long expirationTime);

    public int getVersionID() {
        return getEntryData().getVersion();
    }

    public boolean isTransient() {
        return _transient;
    }

    public void setTransient(boolean _transient) {
        this._transient = _transient;
    }

    public boolean isMaybeUnderXtn() {
        return _maybeUnderXtn;
    }

    public void setMaybeUnderXtn(boolean value) {
        this._maybeUnderXtn = value;
    }

    @Override
    public boolean isDeleted() {
        return _deleted;
    }

    public void setDeleted(boolean isDeleted) {
        this._deleted = isDeleted;
    }


    public void dump(Logger logger, String msg) {
        logger.info(msg);
        logger.info("Start Dumping " + getClass().getName());

        logger.info("Class Name: " + getClassName());
        logger.info("UID: " + getUID());
        logger.info("SCN: " + getSCN());
        logger.info("Transient: " + isTransient());
        logger.info("Expiration Time : " + getExpirationTime());
        logger.info("Deleted: " + isDeleted());
        logger.info("MaybeUnderXtn: " + isMaybeUnderXtn());
        logger.info("HasWaitingFor: " + isHasWaitingFor());
    }

    /*******************************
     * ISelfLockingSubject Members *
     ******************************/

    /**
     * if entryHolder is used as lockObject (for example in ALL_IN_CACHE) - its the lock subject
     * itself
     *
     * @return true if is the subject itself
     */
    public boolean isLockSubject() {
        return true;
    }

    /**
     * if the lock object is an evictable lock object, return the interface (preferable to casing)
     *
     * @return IEvictableLockObject if implemented
     */
    public IEvictableLockObject getEvictableLockObject() {
        // Not relevant for entryHolder as a lock object
        return null;
    }

    /************************
     * IEntryHolder Members *
     ************************/

    public boolean isShadow() {
        return false;
    }

    // Return true if this entry has a shadow entry
    public boolean hasShadow() {
        return hasShadow(false /*safeEntry*/);
    }

    public abstract boolean hasShadow(boolean safeEntry);


    public int getNotifyType() {
        return NotifyModifiers.NOTIFY_NONE;
    }

    public MarshalledObject getHandback() {
        return null;
    }

    public int getOrder() {
        return _order;
    }

    public void setOrder(int order) {
        this._order = order;
    }

    public Object getRoutingValue() {
        IEntryData edata = getEntryData();
        if (edata.getNumOfFixedProperties() == 0)
            return null;
        ITypeDesc typeDesc = edata.getEntryTypeDesc().getTypeDesc();
        String routingPropertyName = typeDesc.getRoutingPropertyName();
        if (routingPropertyName == null)
            return null;
        if (typeDesc.isAutoGenerateRouting())
            return SpaceUidFactory.extractPartitionId(getUID());
        return edata.getPropertyValue(routingPropertyName);
    }

    public Object getEntryId() {
        if (getEntryData().getEntryTypeDesc().getTypeDesc().isAutoGenerateId())
            return getUID();

        int identifierPropertyId = getEntryData().getEntryTypeDesc().getTypeDesc().getIdentifierPropertyId();
        if (identifierPropertyId == -1)
            return null;

        return getEntryData().getFixedPropertyValue(identifierPropertyId);
    }

    @Override
    public void toText(Textualizer textualizer) {
        textualizer.append("typeName", getClassName());
        textualizer.append("uid", getUID());
    }

    @Override
    public String toString() {
        return Textualizer.toString(this);
    }
}
