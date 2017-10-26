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

//
package com.j_spaces.core.cache.blobStore.sadapter;

import com.gigaspaces.datasource.DataIterator;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.metadata.TypeDescriptorUtils;
import com.gigaspaces.internal.server.space.SpaceEngine;
import com.gigaspaces.server.blobstore.BlobStoreGetBulkOperationResult;
import com.gigaspaces.server.blobstore.BlobStoreObjectType;
import com.j_spaces.core.sadapter.ISAdapterIterator;
import com.j_spaces.core.sadapter.SAException;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * off-heap storage adapter meta data iterator to be used in recovery
 *
 * @author yechiel
 * @since 10.0
 */
@com.gigaspaces.api.InternalApi
public class BlobStoreMetaDataIterator implements ISAdapterIterator<ITypeDesc> {
    private final SpaceEngine _engine;
    private final Iterator<ITypeDesc> _iter;
    private final Map<String, BlobStoreStorageAdapterClassInfo> _classesInfo;

    public BlobStoreMetaDataIterator(SpaceEngine engine)
            throws SAException {
        _engine = engine;
        _classesInfo = new HashMap<String, BlobStoreStorageAdapterClassInfo>();
        DataIterator<BlobStoreGetBulkOperationResult> iter = _engine.getCacheManager().getBlobStoreStorageHandler().iterator(BlobStoreObjectType.METADATA);
        if (iter == null) {
            _iter = null;
            return;
        }
        //milk the iterator from blobStore and create list of ordered descriptors
        ITypeDesc typeDesc;
        Map<String, ITypeDesc> typeDescriptors = new HashMap<String, ITypeDesc>();
        try {
            while (true) {
                typeDesc = nextFromBlobStore(iter);
                if (typeDesc == null)
                    break;
                typeDescriptors.put(typeDesc.getTypeName(), typeDesc);
            }
        } finally {
            iter.close();
        }
        if (typeDescriptors.isEmpty()) {
            _iter = null;
            return;
        }
        List<ITypeDesc> result = TypeDescriptorUtils.sort(typeDescriptors);
        _iter = result.iterator();

    }


    @Override
    public ITypeDesc next() throws SAException {
        // TODO Auto-generated method stub
        if (_iter == null)
            return null;
        if (_iter.hasNext())
            return _iter.next();
        return null;
    }

    private ITypeDesc nextFromBlobStore(DataIterator<BlobStoreGetBulkOperationResult> iter) throws SAException {
        // TODO Auto-generated method stub
        BlobStoreGetBulkOperationResult res = null;
        if (iter.hasNext())
            res = iter.next();
        if (res == null)
            return null;
        //NOTE- we dont care about blobStorePosition now
        BlobStoreTypeDescSerializable stored = (BlobStoreTypeDescSerializable) res.getData();
        if (stored != null)
            _classesInfo.put(stored.getTypeDesc().getTypeName(), stored.getBlobStoreStorageAdapterClassInfo());
        return stored != null ? stored.getTypeDesc() : null;
    }

    public Map<String, BlobStoreStorageAdapterClassInfo> getClassesInfo() {
        return _classesInfo;
    }

    @Override
    public void close() throws SAException {
        // TODO Auto-generated method stub
    }
}
