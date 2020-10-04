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
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.metadata.SpacePropertyInfo;
import com.gigaspaces.internal.metadata.SpaceTypeInfo;
import com.gigaspaces.internal.metadata.SpaceTypeInfoRepository;
import com.gigaspaces.internal.query.valuegetter.SpaceEntryPathGetter;
import com.gigaspaces.internal.utils.ReflectionUtils;
import com.gigaspaces.metadata.SpacePropertyDescriptor;
import com.gigaspaces.server.MutableServerEntry;
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
 * Contains all the data (mutable) fields of the entry. when an entry is changed a new IEntryData is
 * created and attached to the IEntryHolder.
 *
 * @author Yechiel Fefer, Niv Ingberg
 * @since 7.0
 */
public interface ITransactionalEntryData extends IEntryData, MutableServerEntry {
    EntryXtnInfo getEntryXtnInfo();

    ITransactionalEntryData createCopy(int newVersion, long newExpiration, EntryXtnInfo newEntryXtnInfo, boolean shallowCloneData);

    ITransactionalEntryData createCopy(IEntryData newEntryData, long newExpirationTime);

    default ITransactionalEntryData createCopyWithoutTxnInfo() {
        return createCopy(getVersion(), getExpirationTime(), null, false);
    }

    default ITransactionalEntryData createCopyWithoutTxnInfo(long newExpirationTime) {
        return createCopy(getVersion(), newExpirationTime, null, false);
    }

    default ITransactionalEntryData createCopyWithTxnInfo(boolean createEmptyTxnInfo) {
        return createCopy(getVersion(), getExpirationTime(), copyTxnInfo(true, createEmptyTxnInfo), false);
    }

    default ITransactionalEntryData createCopyWithTxnInfo(int newVersion, long newExpirationTime) {
        return createCopy(newVersion, newExpirationTime, copyTxnInfo(true, false), false);
    }

    default ITransactionalEntryData createCopyWithSuppliedTxnInfo(EntryXtnInfo ex) {
        return createCopy(getVersion(), getExpirationTime(), ex, false);
    }

    default ITransactionalEntryData createShallowClonedCopyWithSuppliedVersion(int versionID) {
        return createCopy(versionID, getExpirationTime(), copyTxnInfo(true, false), true);
    }

    default ITransactionalEntryData createShallowClonedCopyWithSuppliedVersionAndExpiration(int versionID, long expirationTime) {
        return createCopy(versionID, expirationTime, copyTxnInfo(true, false), true);
    }

    @Override
    default void setPathValue(String path, Object value) {
        ITypeDesc typeDesc = getSpaceTypeDescriptor();
        if (!path.contains(".")) {
            if (typeDesc.getIdPropertyName().equals(path))
                throwChangeIdException(value);

            int pos = typeDesc.getFixedPropertyPosition(path);
            if (pos >= 0) {
                SpacePropertyDescriptor fixedProperty = typeDesc.getFixedProperty(pos);
                if (value == null) {
                    validateCanSetNull(path, pos, fixedProperty);
                } else {
                    boolean illegalAssignment = false;
                    if (ReflectionUtils.isPrimitive(fixedProperty.getTypeName())) {
                        illegalAssignment = !ReflectionUtils.isPrimitiveAssignable(fixedProperty.getTypeName(),
                                value.getClass());
                    } else {
                        illegalAssignment = !fixedProperty.getType().isAssignableFrom(value.getClass());
                    }

                    if (illegalAssignment)
                        throw new IllegalArgumentException("Cannot set value [" + value +
                                "] of class [" + value.getClass() +
                                "] to property '" + path +
                                "' of class [" + fixedProperty.getType() + "]");
                }

                setFixedPropertyValue(pos, value);
            } else if (typeDesc.supportsDynamicProperties())
                setDynamicPropertyValue(path, value);

            else throw new IllegalArgumentException("Unknown property name '" + path + "'");
        } else {
            String rootPropertyName = path.substring(0, path.indexOf("."));
            if (typeDesc.getIdPropertyName().equals(rootPropertyName))
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

    @Override
    default void unsetPath(String path) {
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

    void setDynamicPropertyValue(String propertyName, Object value);

    default void unsetDynamicPropertyValue(String propertyName) {
        Map<String, Object> dynamicProperties = getDynamicProperties();
        if (dynamicProperties != null)
            dynamicProperties.remove(propertyName);
    }

    default void validateCanSetNull(String path, int pos, SpacePropertyDescriptor fixedProperty) {
        if (ReflectionUtils.isPrimitive(fixedProperty.getTypeName())
                && !getSpaceTypeDescriptor().getIntrospector(null)
                .propertyHasNullValue(pos))
            throw new IllegalArgumentException("Cannot set null to property '"
                    + path
                    + "' of class ["
                    + fixedProperty.getType()
                    + "] because it has no null value defined");
    }

    default void throwChangeIdException(Object value) {
        Object currentId = getPropertyValue(getSpaceTypeDescriptor().getIdPropertyName());
        throw new UnsupportedOperationException("Attempting to change the id property named '"
                + getSpaceTypeDescriptor().getIdPropertyName()
                + "' of type '"
                + getSpaceTypeDescriptor().getTypeName()
                + "' which has a current value of [" + currentId + "] with a new value ["
                + value
                + "]. Changing the id property of an existing entry is not allowed.");
    }

    default void deepCloneProperty(String rootPropertyName) {
        Object propertyValue = getPropertyValue(rootPropertyName);
        Object cloneValue = DefaultValueCloner.get().cloneValue(propertyValue, false /* isClonable */, null, "", getSpaceTypeDescriptor().getTypeName());
        setPathValue(rootPropertyName, cloneValue);
    }

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

    default EntryXtnInfo copyTxnInfo(boolean cloneXtnInfo, boolean createEmptyTxnInfoIfNon) {
        EntryXtnInfo entryXtnInfo = getEntryXtnInfo();
        if (entryXtnInfo != null) {
            return cloneXtnInfo ? new EntryXtnInfo(entryXtnInfo) : entryXtnInfo;
        }
        return createEmptyTxnInfoIfNon ? new EntryXtnInfo() : null;
    }

    default Collection<ITemplateHolder> getWaitingFor() {
        EntryXtnInfo entryXtnInfo = getEntryXtnInfo();
        return entryXtnInfo == null ? null : entryXtnInfo.getWaitingFor();
    }

    default void initWaitingFor() {
        getEntryXtnInfo().initWaitingFor();
    }

    default boolean isExpired() {
        return isExpired(SystemTime.timeMillis());
    }

    default boolean isExpired(long limit) {
        long leaseToCompare = getExpirationTime();
        if (getWriteLockOwner() != null && getOtherUpdateUnderXtnEntry() != null) {
            //take the time from the original entry in case pending update under xtn
            IEntryHolder original = getOtherUpdateUnderXtnEntry();
            if (original != null)
                leaseToCompare = Math.max(leaseToCompare, original.getEntryData().getExpirationTime());
        }
        return leaseToCompare < limit;
    }
}
