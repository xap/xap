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
package com.j_spaces.core.cache.blobStore;


import com.gigaspaces.server.blobstore.BlobStoreException;
import com.gigaspaces.server.blobstore.BlobStoreObjectType;
import com.j_spaces.core.cache.CacheManager;

import org.jini.rio.boot.ServiceClassLoader;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamClass;
import java.io.OutputStream;
import java.io.Serializable;


/**
 * Created by kobi on 4/10/14.
 */
@com.gigaspaces.api.InternalApi
public class BlobStoreSerializationUtils {

    private final static ThreadLocal<ByteArrayOutputStream> threadLocal = new ThreadLocal<ByteArrayOutputStream>();

    private final CacheManager _cacheManager;

    public BlobStoreSerializationUtils(CacheManager _cacheManager) {
        this._cacheManager = _cacheManager;
    }

    public byte[] serialize(Serializable obj, BlobStoreObjectType objectType) {
        ByteArrayOutputStream baos = threadLocal.get();
        if (baos == null) {
            baos = new ByteArrayOutputStream(512);
            baos.reset();
            threadLocal.set(baos);
        } else {
            baos.reset();
        }
        serialize(obj, baos, objectType);
        return baos.toByteArray();
    }

    private void serialize(Serializable obj, OutputStream outputStream, BlobStoreObjectType objectType) {
        if (outputStream == null) {
            throw new IllegalArgumentException("The OutputStream must not be null");
        }
        ObjectOutputStream out = null;
        try {
            // stream closed in the finally
            out = new ObjectOutputStream(outputStream);
            if (objectType != BlobStoreObjectType.DATA) {
                out.writeObject(obj);
            } else {//BlobStoreObjectType.DATA
                BlobStoreEntryLayout el = (BlobStoreEntryLayout) obj;
                el.writeExternal(out, _cacheManager);
            }
        } catch (RuntimeException rte) {
            throw new BlobStoreException(rte);
        } catch (IOException ex) {
            throw new BlobStoreException(ex);
        } finally {
            try {
                if (out != null) {
                    out.close();
                }
            } catch (IOException ex) {
                // ignore close exception
            }
        }
    }

    public Serializable deserialize(Serializable input, BlobStoreObjectType objectType, boolean initialLoad,  boolean onlyIndexedPart, IBlobStoreOffHeapInfo offHeapInfo) {
        if (input == null) {
            throw new IllegalArgumentException("input must not be null");
        }
        return deserialize((byte[]) input, objectType, initialLoad, onlyIndexedPart, offHeapInfo);
    }


    private Serializable deserialize(InputStream inputStream, BlobStoreObjectType objectType, boolean initialLoad, boolean onlyIndexedPart, IBlobStoreOffHeapInfo offHeapInfo) {
        if (inputStream == null) {
            throw new IllegalArgumentException("The InputStream must not be null");
        }
        ObjectInputStream in = null;
        try {
            // stream closed in the finally
            in = new ClassLoaderAwareInputStream(inputStream);
            if (objectType != BlobStoreObjectType.DATA) {
                return (Serializable) in.readObject();
            }
            {//BlobStoreObjectType.DATA
                BlobStoreEntryLayout el = new BlobStoreEntryLayout();
                el.readExternal(in, _cacheManager, initialLoad, onlyIndexedPart, offHeapInfo);
                return el;
            }

        } catch (RuntimeException rte) {
            throw new BlobStoreException(rte);
        } catch (ClassNotFoundException ex) {
            throw new BlobStoreException(ex);
        } catch (IOException ex) {
            throw new BlobStoreException(ex);
        } finally {
            try {
                if (in != null) {
                    in.close();
                }
            } catch (IOException ex) {
                // ignore close exception
            }
        }
    }

    private Serializable deserialize(byte[] objectData, BlobStoreObjectType objectType, boolean initialLoad, boolean onlyIndexedPart, IBlobStoreOffHeapInfo offHeapInfo) {
        ByteArrayInputStream bais = new ByteArrayInputStream(objectData);
        return deserialize(bais, objectType, initialLoad, onlyIndexedPart, offHeapInfo);
    }


    private class ClassLoaderAwareInputStream extends ObjectInputStream {
        private ClassLoaderAwareInputStream(InputStream in) throws IOException {
            super(in);
        }

        @Override
        public Class resolveClass(ObjectStreamClass desc) throws IOException,
                ClassNotFoundException {
            ClassLoader currentTccl = Thread.currentThread().getContextClassLoader();
            if (currentTccl instanceof ServiceClassLoader) {
                try {
                    return Class.forName(desc.getName(), false, currentTccl);
                } catch (Exception e) {
                }
            }
            return super.resolveClass(desc);
        }
    }
}


