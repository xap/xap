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
package com.gigaspaces.server.blobstore;


import com.j_spaces.core.cache.blobStore.IBlobStoreOffHeapInfo;

/**
 * result an operation executed as part of a bulk on blob-store storage (SSD, off-heap buffers)
 *
 * @author yechielf
 * @since 10.0
 */

public abstract class BlobStoreBulkOperationResult {
    private final BlobStoreBulkOperationType _opType;
    private final java.io.Serializable _id;
    private java.io.Serializable _data;
    private final Object _position;
    private final Throwable _exception;


    public BlobStoreBulkOperationResult(BlobStoreBulkOperationType opType, java.io.Serializable id,
                                        java.io.Serializable data, Object position) {
        _opType = opType;
        _id = id;
        _data = data;
        _position = position;
        _exception = null;
    }

    public BlobStoreBulkOperationResult(BlobStoreBulkOperationType opType, java.io.Serializable id, Throwable exception) {

        _opType = opType;
        _id = id;
        _data = null;
        _position = null;
        _exception = exception;
    }


    public BlobStoreBulkOperationType getOpType() {
        return _opType;
    }

    public java.io.Serializable getId() {
        return _id;
    }

    public java.io.Serializable getData() {
        return _data;
    }

    public void setData(java.io.Serializable data) {
        _data = data;
    }

    public Object getPosition() {
        return _position;
    }

    public Throwable getException() {
        return _exception;
    }

    public IBlobStoreOffHeapInfo getOffHeapInfo() {
        return null;
    }
}
