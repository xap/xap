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
package com.j_spaces.core.cache.blobStore.storage;

import com.gigaspaces.datasource.DataIterator;
import com.gigaspaces.server.blobstore.BlobStoreConfig;
import com.gigaspaces.server.blobstore.BlobStoreGetBulkOperationResult;
import com.gigaspaces.server.blobstore.BlobStoreObjectType;
import com.gigaspaces.server.blobstore.BlobStoreStorageHandler;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

@com.gigaspaces.api.InternalApi
public class BlobStoreHashMock extends BlobStoreStorageHandler {

    private ConcurrentHashMap<Serializable, Serializable> _data;
    private ConcurrentHashMap<Serializable, Serializable> _metadata;
    private ConcurrentHashMap<Serializable, Serializable> _sync;
    private ConcurrentHashMap<Serializable, Serializable> _syncovf;
    private ConcurrentHashMap<Serializable, Serializable> _admin;


    private Serializable marsh(Serializable data) {
        //no need to- general serialization is performed
        return data;
        /*
        MarshalledObject m =null;
		try
		{
		m = new MarshalledObject(data);
		}
		catch (Exception ex)
		{ throw new RuntimeException(ex);}
		return m;
		*/
    }

    private Serializable unmarsh(Serializable data) {
        //no need to- general serialization is performed
        return data;
		/*
		MarshalledObject m =(MarshalledObject)data;
		try
		{
		return (Serializable)m.get();
		}
		catch (Exception ex)
		{ throw new RuntimeException(ex);}
		*/
    }

    @Override
    public void initialize(BlobStoreConfig blobStoreConfig) {
        // TODO Auto-generated method stub
        if (_data == null) {
            _data = new ConcurrentHashMap<Serializable, Serializable>(16, 0.75f, 128);
            _metadata = new ConcurrentHashMap<Serializable, Serializable>();
            _syncovf = new ConcurrentHashMap<Serializable, Serializable>(16, 0.75f, 128);
            _sync = new ConcurrentHashMap<Serializable, Serializable>(16, 0.75f, 128);
            _admin = new ConcurrentHashMap<Serializable, Serializable>(16, 0.75f, 128);

            System.out.println("performed init on BlobStoreHashMock___");
        } else
            System.out.println("performed WARM init on BlobStoreHashMock___");
    }


    @Override
    public Object add(Serializable id, Serializable data,
                      BlobStoreObjectType objectType) {
        // TODO Auto-generated method stub
        if (objectType == BlobStoreObjectType.SYNC)
            return add_impl(id, data, _sync);
        if (objectType == BlobStoreObjectType.SYNC_OVERFLOW)
            return add_impl(id, data, _syncovf);
        if (objectType == BlobStoreObjectType.ADMIN)
            return add_impl(id, data, _admin);
        return objectType == BlobStoreObjectType.DATA ? add_impl(id, data, _data) : add_impl(id, data, _metadata);
    }

    private Object add_impl(java.io.Serializable id, java.io.Serializable data, ConcurrentHashMap<Serializable, Serializable> store) {
        // TODO Auto-generated method stub
        data = marsh(data);
        if (store.putIfAbsent(id, data) != null)
            throw new RuntimeException("key already exist " + id);

        return -1;
    }


    @Override
    public Serializable get(Serializable id, Object position,
                            BlobStoreObjectType objectType) {
        // TODO Auto-generated method stub
        if (objectType == BlobStoreObjectType.SYNC)
            return get_impl(id, position, _sync);
        if (objectType == BlobStoreObjectType.SYNC_OVERFLOW)
            return get_impl(id, position, _syncovf);
        if (objectType == BlobStoreObjectType.ADMIN)
            return get_impl(id, position, _admin);
        return objectType == BlobStoreObjectType.DATA ? get_impl(id, position, _data) : get_impl(id, position, _metadata);
    }

    private Serializable get_impl(java.io.Serializable id, Object position, ConcurrentHashMap<Serializable, Serializable> store) {
        // TODO Auto-generated method stub
        Serializable o = store.get(id);
//		if (o ==null && store == _data)
//				throw new RuntimeException("no key exist " + id);

        return o != null ? unmarsh(o) : null;
    }


    @Override
    public Object replace(Serializable id, Serializable data, Object position,
                          BlobStoreObjectType objectType) {
        // TODO Auto-generated method stub
        if (objectType == BlobStoreObjectType.SYNC)
            return replace_impl(id, data, position, _sync);
        if (objectType == BlobStoreObjectType.SYNC_OVERFLOW)
            return replace_impl(id, data, null, _syncovf);
        if (objectType == BlobStoreObjectType.ADMIN)
            return replace_impl(id, data, null, _admin);
        return objectType == BlobStoreObjectType.DATA ? replace_impl(id, data, position, _data) : replace_impl(id, data, position, _metadata);
    }


    private Object replace_impl(java.io.Serializable id, java.io.Serializable data, Object position,
                                ConcurrentHashMap<Serializable, Serializable> store) {
        data = marsh(data);
        if (store.replace(id, data) == null)
            throw new RuntimeException("no key exist " + id);
        return -1;

    }


    @Override
    public Serializable remove(Serializable id, Object position,
                               BlobStoreObjectType objectType) {
        // TODO Auto-generated method stub
        if (objectType == BlobStoreObjectType.SYNC)
            return remove_impl(id, null, _sync);
        if (objectType == BlobStoreObjectType.SYNC_OVERFLOW)
            return remove_impl(id, null, _syncovf);
        if (objectType == BlobStoreObjectType.DATA)
            return remove_impl(id, position, _data);
        if (objectType == BlobStoreObjectType.ADMIN)
            return remove_impl(id, position, _admin);
        else
            return remove_impl(id, position, _metadata);
    }

    private Serializable remove_impl(java.io.Serializable id, Object position, ConcurrentHashMap<Serializable, Serializable> store) {
        Serializable o = store.remove(id);
        if (o == null)
            throw new RuntimeException("remove: didn't find object with id=" + id);
        return unmarsh(o);
    }

    @Override
    public void close() {
        // TODO Auto-generated method stub

    }

    @Override
    public DataIterator<BlobStoreGetBulkOperationResult> iterator(BlobStoreObjectType objectType) {
        if (objectType == BlobStoreObjectType.SYNC)
            return new MockIter(_sync);
        if (objectType == BlobStoreObjectType.SYNC_OVERFLOW)
            return new MockIter(_syncovf);
        if (objectType == BlobStoreObjectType.ADMIN)
            return new MockIter(_admin);
        return objectType == BlobStoreObjectType.DATA ? new MockIter(_data) : new MockIter(_metadata);
    }


    public class MockIter
            implements DataIterator<BlobStoreGetBulkOperationResult> {
        private final ConcurrentHashMap<Serializable, Serializable> _map;
        private final Iterator<Entry<Serializable, Serializable>> _iter;

        MockIter(ConcurrentHashMap<Serializable, Serializable> map) {
            _map = map;
            _iter = _map.entrySet().iterator();
        }

        public boolean hasNext() {
            return _iter.hasNext();
        }

        public void remove() {
        }

        ;

        public BlobStoreGetBulkOperationResult next() {
            Entry<Serializable, Serializable> e = _iter.next();
            return new BlobStoreGetBulkOperationResult(e.getKey(), unmarsh(e.getValue()), null, null);
        }

        public void close() {
        }
    }
}
