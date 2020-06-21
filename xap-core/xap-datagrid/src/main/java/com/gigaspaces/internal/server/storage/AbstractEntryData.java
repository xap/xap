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

import com.gigaspaces.entry.VirtualEntry;
import com.gigaspaces.internal.lease.LeaseUtils;
import com.gigaspaces.internal.metadata.EntryTypeDesc;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.metadata.SpacePropertyInfo;
import com.gigaspaces.internal.metadata.SpaceTypeInfo;
import com.gigaspaces.internal.metadata.SpaceTypeInfoRepository;
import com.gigaspaces.internal.query.valuegetter.SpaceEntryPathGetter;
import com.gigaspaces.internal.utils.ReflectionUtils;
import com.gigaspaces.metadata.SpacePropertyDescriptor;
import com.gigaspaces.time.SystemTime;
import com.j_spaces.core.SpaceOperations;
import com.j_spaces.core.XtnEntry;
import com.j_spaces.core.cache.DefaultValueCloner;
import com.j_spaces.core.server.transaction.EntryXtnInfo;

import net.jini.core.transaction.server.ServerTransaction;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Contains all the data (mutable) fields of the entry. when an entry is changed a new EntryData is
 * created and attached to the EntryHolder
 *
 * @author Yechiel Fefer
 * @version 1.0
 * @since 7.0
 */
public abstract class AbstractEntryData implements ITransactionalEntryData {
    protected final EntryTypeDesc _entryTypeDesc;
    protected final int _versionID;
    protected final long _expirationTime;
    private final EntryXtnInfo _entryTxnInfo;

    protected AbstractEntryData(EntryTypeDesc entryTypeDesc, int version, long expirationTime, boolean createEmptyTxnInfoIfNon) {
        this._entryTypeDesc = entryTypeDesc;
        this._versionID = version;
        this._expirationTime = expirationTime;
        this._entryTxnInfo = createEmptyTxnInfoIfNon ? new EntryXtnInfo() : null;
    }

    protected AbstractEntryData(EntryTypeDesc entryTypeDesc, int version, long expirationTime, boolean cloneXtnInfo, ITransactionalEntryData other, boolean createEmptyTxnInfoIfNon) {
        this._entryTypeDesc = entryTypeDesc;
        this._versionID = version;
        this._expirationTime = expirationTime;
        this._entryTxnInfo = copyTxnInfo(other.getEntryXtnInfo(), cloneXtnInfo, createEmptyTxnInfoIfNon);
    }

    private static EntryXtnInfo copyTxnInfo(EntryXtnInfo otherEntryTxnInfo, boolean cloneXtnInfo, boolean createEmptyTxnInfoIfNon) {
        if (otherEntryTxnInfo != null) {
            return cloneXtnInfo ? new EntryXtnInfo(otherEntryTxnInfo) : otherEntryTxnInfo;
        }
        return createEmptyTxnInfoIfNon ? new EntryXtnInfo() : null;
    }

    protected AbstractEntryData(ITransactionalEntryData other, EntryXtnInfo xtnInfo) {
        this._entryTypeDesc = other.getEntryTypeDesc();
        this._versionID = other.getVersion();
        this._expirationTime = other.getExpirationTime();
        this._entryTxnInfo = xtnInfo;
    }

    @Override
    public ITypeDesc getSpaceTypeDescriptor() {
        return _entryTypeDesc.getTypeDesc();
    }

    @Override
    public EntryTypeDesc getEntryTypeDesc() {
        return _entryTypeDesc;
    }

    public Object getPropertyValue(String name) {
        ITypeDesc typeDesc = _entryTypeDesc.getTypeDesc();
        int pos = typeDesc.getFixedPropertyPosition(name);
        if (pos != -1)
            return getFixedPropertyValue(pos);

        if (typeDesc.supportsDynamicProperties()) {
            Map<String, Object> dynamicProperties = getDynamicProperties();
            return dynamicProperties != null ? dynamicProperties.get(name) : null;
        }

        throw new IllegalArgumentException("Unknown property name '" + name + "' in type " + getSpaceTypeDescriptor().getTypeName());
    }

    public int getVersion() {
        return _versionID;
    }

    public long getExpirationTime() {
        return _expirationTime;
    }

    public long getTimeToLive(boolean useDummyIfRelevant) {
        return LeaseUtils.getTimeToLive(_expirationTime, useDummyIfRelevant);
    }

    public static long getTimeToLive(long expirationTime, boolean useDummyIfRelevant) {
        return LeaseUtils.getTimeToLive(expirationTime, useDummyIfRelevant);
    }

    public EntryXtnInfo getEntryXtnInfo() {
        return _entryTxnInfo;
    }

    public boolean anyReadLockXtn() {
        return _entryTxnInfo == null ? false : _entryTxnInfo.anyReadLockXtn();
    }

    public List<XtnEntry> getReadLocksOwners() {
        return _entryTxnInfo == null ? null : _entryTxnInfo.getReadLocksOwners();
    }

    public void addReadLockOwner(XtnEntry xtn) {
        _entryTxnInfo.addReadLockOwner(xtn);
    }

    public void removeReadLockOwner(XtnEntry xtn) {
        if (_entryTxnInfo != null)
            _entryTxnInfo.removeReadLockOwner(xtn);
    }

    public void clearReadLockOwners() {
        if (_entryTxnInfo != null)
            _entryTxnInfo.clearReadLockOwners();
    }

    public XtnEntry getWriteLockOwner() {
        return _entryTxnInfo == null ? null : _entryTxnInfo.getWriteLockOwner();
    }

    public void setWriteLockOwner(XtnEntry writeLockOwner) {
        if (_entryTxnInfo == null && writeLockOwner != null)
            throw new RuntimeException("entryTxnInfo is null");
        if (_entryTxnInfo != null)
            _entryTxnInfo.setWriteLockOwner(writeLockOwner);
    }

    public ServerTransaction getWriteLockTransaction() {
        XtnEntry owner = getWriteLockOwner();
        return owner == null ? null : owner.m_Transaction;
    }

    public int getWriteLockOperation() {
        return _entryTxnInfo == null ? SpaceOperations.NOOP : _entryTxnInfo.getWriteLockOperation();
    }

    public void setWriteLockOperation(int writeLockOperation) {
        _entryTxnInfo.setWriteLockOperation(writeLockOperation);
    }

    public XtnEntry getXidOriginated() {
        return _entryTxnInfo == null ? null : _entryTxnInfo.getXidOriginated();
    }

    public void setXidOriginated(XtnEntry xidOriginated) {
        if (_entryTxnInfo == null && xidOriginated != null)
            throw new RuntimeException("entryTxnInfo is null");
        if (_entryTxnInfo != null)
            _entryTxnInfo.setXidOriginated(xidOriginated);
    }

    public ServerTransaction getXidOriginatedTransaction() {
        XtnEntry originated = getXidOriginated();
        return originated == null ? null : originated.m_Transaction;
    }

    public IEntryHolder getOtherUpdateUnderXtnEntry() {
        return _entryTxnInfo == null ? null : _entryTxnInfo.getOtherUpdateUnderXtnEntry();
    }

    public void setOtherUpdateUnderXtnEntry(IEntryHolder eh) {
        if (_entryTxnInfo == null) {
            if (eh == null)
                return;
            throw new RuntimeException("entryTxnInfo is null");
        }
        _entryTxnInfo.setOtherUpdateUnderXtnEntry(eh);
    }

    public Collection<ITemplateHolder> getWaitingFor() {
        return _entryTxnInfo == null ? null : _entryTxnInfo.getWaitingFor();
    }

    public void initWaitingFor() {
        _entryTxnInfo.initWaitingFor();
    }

    public boolean isExpired(long limit) {
        long leaseToCompare = getExpirationTime();
        if (getWriteLockOwner() != null && getOtherUpdateUnderXtnEntry() != null) {
            //take the time from the original entry in case pending update under xtn
            IEntryHolder original = getOtherUpdateUnderXtnEntry();
            if (original != null)
                leaseToCompare = Math.max(leaseToCompare, original.getEntryData().getExpirationTime());
        }
        return leaseToCompare < limit;
    }

    public boolean isExpired() {
        return isExpired(SystemTime.timeMillis());
    }

    @Override
    public Object getPathValue(String path) {
        if (!path.contains("."))
            return getPropertyValue(path);
        return new SpaceEntryPathGetter(path).getValue(this);
    }

    @Override
    public void setPathValue(String path, Object value) {
        if (!path.contains(".")) {
            if (getSpaceTypeDescriptor().getIdPropertyName().equals(path))
                throwChangeIdException(value);

            int pos = getSpaceTypeDescriptor().getFixedPropertyPosition(path);
            if (pos >= 0) {
                SpacePropertyDescriptor fixedProperty = getSpaceTypeDescriptor().getFixedProperty(pos);
                if (value == null) {
                    validateCanSetNull(path, pos, fixedProperty);
                } else {
                    boolean illegalAssignment = false;
                    if (ReflectionUtils.isPrimitive(fixedProperty.getTypeName())) {
                        illegalAssignment = !ReflectionUtils.isPrimitiveAssignable(fixedProperty.getTypeName(),
                                value.getClass());
                    } else {
                        illegalAssignment = !fixedProperty.getType()
                                .isAssignableFrom(value.getClass());
                    }

                    if (illegalAssignment)
                        throw new IllegalArgumentException("Cannot set value ["
                                + value + "] of class [" + value.getClass()
                                + "] to property '" + path + "' of class ["
                                + fixedProperty.getType() + "]");
                }

                setFixedPropertyValue(pos, value);
            } else if (getSpaceTypeDescriptor().supportsDynamicProperties())
                setDynamicPropertyValue(path, value);

            else throw new IllegalArgumentException("Unknown property name '" + path + "'");
        } else {
            String rootPropertyName = path.substring(0, path.indexOf("."));
            if (getSpaceTypeDescriptor().getIdPropertyName().equals(rootPropertyName))
                throwChangeIdException(value);

            deepCloneProperty(rootPropertyName);
            int propertyNameSeperatorIndex = path.lastIndexOf(".");
            String pathToParent = path.substring(0, propertyNameSeperatorIndex);
            String propertyName = path.substring(propertyNameSeperatorIndex + 1);
            Object valueParent = new SpaceEntryPathGetter(pathToParent).getValue(this);
            if (valueParent instanceof Map)
                ((Map) valueParent).put(propertyName, value);
            else if (valueParent instanceof VirtualEntry)
                ((VirtualEntry) valueParent).setProperty(propertyName, value);
            else {
                Class<? extends Object> type = valueParent.getClass();
                SpaceTypeInfo typeInfo = SpaceTypeInfoRepository.getTypeInfo(type);
                SpacePropertyInfo propertyInfo = typeInfo.getProperty(propertyName);
                if (propertyInfo == null)
                    throw new IllegalArgumentException("Property '" + propertyName + "' is not a member of " + type.getName() + " in '" + path + "'");
                propertyInfo.setValue(valueParent, value);
            }
        }
    }

    private void validateCanSetNull(String path, int pos,
                                    SpacePropertyDescriptor fixedProperty) {
        if (ReflectionUtils.isPrimitive(fixedProperty.getTypeName())
                && !getSpaceTypeDescriptor().getIntrospector(null)
                .propertyHasNullValue(pos))
            throw new IllegalArgumentException("Cannot set null to property '"
                    + path
                    + "' of class ["
                    + fixedProperty.getType()
                    + "] because it has no null value defined");
    }

    private void throwChangeIdException(Object value) {
        Object currentId = getPropertyValue(getSpaceTypeDescriptor().getIdPropertyName());
        throw new UnsupportedOperationException("Attempting to change the id property named '"
                + getSpaceTypeDescriptor().getIdPropertyName()
                + "' of type '"
                + getSpaceTypeDescriptor().getTypeName()
                + "' which has a current value of [" + currentId + "] with a new value ["
                + value
                + "]. Changing the id property of an existing entry is not allowed.");
    }

    @Override
    public void unsetPath(String path) {
        if (!path.contains(".")) {
            int pos = getSpaceTypeDescriptor().getFixedPropertyPosition(path);
            if (pos >= 0) {
                SpacePropertyDescriptor fixedProperty = getSpaceTypeDescriptor().getFixedProperty(pos);
                validateCanSetNull(path, pos, fixedProperty);
                setFixedPropertyValue(pos, null);
            } else if (getSpaceTypeDescriptor().supportsDynamicProperties())
                unsetDynamicPropertyValue(path);

            else throw new IllegalArgumentException("Unknown property name '" + path + "'");
        } else {
            String rootPropertyName = path.substring(0, path.indexOf("."));
            if (getSpaceTypeDescriptor().getIdPropertyName().equals(rootPropertyName)) {
                throw new UnsupportedOperationException("Attempting to unset the id property named '"
                        + getSpaceTypeDescriptor().getIdPropertyName()
                        + "' of type '"
                        + getSpaceTypeDescriptor().getTypeName()
                        + "' which has a current value of [" + getPropertyValue(getSpaceTypeDescriptor().getIdPropertyName()) + "]. Changing the id property of an existing entry is not allowed.");
            }
            deepCloneProperty(rootPropertyName);
            int propertyNameSeperatorIndex = path.lastIndexOf(".");
            String pathToParent = path.substring(0, propertyNameSeperatorIndex);
            String propertyName = path.substring(propertyNameSeperatorIndex + 1);
            Object valueParent = new SpaceEntryPathGetter(pathToParent).getValue(this);
            if (valueParent instanceof Map)
                ((Map) valueParent).remove(propertyName);
            else if (valueParent instanceof VirtualEntry)
                ((VirtualEntry) valueParent).removeProperty(propertyName);
            else {
                Class<? extends Object> type = valueParent.getClass();
                SpaceTypeInfo typeInfo = SpaceTypeInfoRepository.getTypeInfo(type);
                SpacePropertyInfo propertyInfo = typeInfo.getProperty(propertyName);
                if (propertyInfo == null)
                    throw new IllegalArgumentException("Property '" + propertyName + "' is not a member of " + type.getName() + " in '" + path + "'");
                propertyInfo.setValue(valueParent, null);
            }
        }

    }

    private void deepCloneProperty(String rootPropertyName) {
        Object propertyValue = getPropertyValue(rootPropertyName);
        Object cloneValue = DefaultValueCloner.get().cloneValue(propertyValue, false /* isClonable */, null, "", getSpaceTypeDescriptor().getTypeName());
        setPathValue(rootPropertyName, cloneValue);
    }
}
