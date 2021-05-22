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

package com.j_spaces.core.cache.blobStore.sadapter;

import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.metadata.TypeDesc;
import com.gigaspaces.internal.metadata.TypeDescVersionedSerializable;
import com.gigaspaces.serialization.SmartExternalizable;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * BlobStore storage adapter storedTypeDesc
 *
 * @author yechiel
 * @since 10.1
 */
@com.gigaspaces.api.InternalApi
public class BlobStoreTypeDescSerializable implements SmartExternalizable {
    private static final long serialVersionUID = -8263097363680036604L;

    private TypeDescVersionedSerializable _typeDescWrapper;
    private BlobStoreStorageAdapterClassInfo _blobStoreStorageAdapterClassInfo;

    /* For Externalizable */
    public BlobStoreTypeDescSerializable() {
    }

    public BlobStoreTypeDescSerializable(TypeDesc typeDesc, BlobStoreStorageAdapterClassInfo classInfo) {
        _typeDescWrapper = new TypeDescVersionedSerializable(typeDesc);
        _blobStoreStorageAdapterClassInfo = classInfo;
    }


    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        _blobStoreStorageAdapterClassInfo.writeExternal(out);
        _typeDescWrapper.writeExternal(out);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        _blobStoreStorageAdapterClassInfo = new BlobStoreStorageAdapterClassInfo();
        _blobStoreStorageAdapterClassInfo.readExternal(in);
        _typeDescWrapper = new TypeDescVersionedSerializable();
        _typeDescWrapper.readExternal(in);

    }

    public ITypeDesc getTypeDesc() {
        return _typeDescWrapper.getTypeDesc();
    }

    public BlobStoreStorageAdapterClassInfo getBlobStoreStorageAdapterClassInfo() {
        return _blobStoreStorageAdapterClassInfo;
    }
}
