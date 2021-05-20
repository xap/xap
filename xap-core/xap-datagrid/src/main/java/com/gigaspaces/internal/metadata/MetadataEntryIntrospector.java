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

package com.gigaspaces.internal.metadata;

import com.j_spaces.core.client.EntryInfo;
import com.j_spaces.core.client.MetaDataEntry;

/**
 * MetadataEntry introspector for all the metadataEntry introspectors implementations.
 *
 * @author Niv Ingberg
 * @since 7.0
 */
@com.gigaspaces.api.InternalApi
public class MetadataEntryIntrospector<T extends MetaDataEntry> extends EntryIntrospector<T> {
    // serialVersionUID should never be changed.
    private static final long serialVersionUID = 1L;
    public static final byte EXTERNALIZABLE_CODE = 3;

    /**
     * Default constructor for Externalizable.
     */
    public MetadataEntryIntrospector() {
    }

    public MetadataEntryIntrospector(ITypeDesc typeDesc) {
        super(typeDesc);
    }

    @Override
    public boolean hasTransientProperty(T target) {
        return true;
    }

    @Override
    public boolean isTransient(T target) {
        if (target == null)
            return false;
        return target.isTransient();
    }

    @Override
    public boolean setTransient(T target, boolean isTransient) {
        if (isTransient)
            target.makeTransient();
        else
            target.makePersistent();

        return true;
    }

    @Override
    public boolean hasTimeToLiveProperty(T target) {
        return target.__getEntryInfo() != null;
    }

    @Override
    public long getTimeToLive(T target) {
        EntryInfo info = target.__getEntryInfo();
        if (info != null)
            return info.m_TimeToLive;

        return 0;
    }

    @Override
    public boolean setTimeToLive(T target, long ttl) {
        EntryInfo info = target.__getEntryInfo();
        if (info != null) {
            info.m_TimeToLive = ttl;
            return true;
        }

        return false;
    }

    @Override
    protected EntryInfo getEntryInfo(T target) {
        return target.__getEntryInfo();
    }

    @Override
    public void setEntryInfo(T target, String uid, int version, long ttl) {
        target.__setEntryInfo(new EntryInfo(uid, version, ttl));
    }

    @Override
    public byte getExternalizableCode() {
        return EXTERNALIZABLE_CODE;
    }
}
