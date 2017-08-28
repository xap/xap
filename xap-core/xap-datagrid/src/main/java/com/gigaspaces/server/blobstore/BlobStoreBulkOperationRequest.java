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


/**
 * base for an operation to execute as part of a bulk on off heap storage (SSD, off-heap buffers)
 *
 * @author yechielf
 * @since 10.0
 */
public abstract class BlobStoreBulkOperationRequest {
    private final BlobStoreBulkOperationType _opType;
    private final java.io.Serializable _id;
    private java.io.Serializable _data;
    private final Object _position;



    BlobStoreBulkOperationRequest(BlobStoreBulkOperationType opType, java.io.Serializable id,
                                  java.io.Serializable data, Object position) {
        _opType = opType;
        _id = id;
        _data = data;
        _position = position;
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        BlobStoreBulkOperationRequest that = (BlobStoreBulkOperationRequest) o;

        if (_opType != that._opType) return false;
        if (_id != null ? !_id.equals(that._id) : that._id != null) return false;
        if (_data != null ? !_data.equals(that._data) : that._data != null) return false;
        return _position != null ? _position.equals(that._position) : that._position == null;
    }

    @Override
    public int hashCode() {
        int result = _opType != null ? _opType.hashCode() : 0;
        result = 31 * result + (_id != null ? _id.hashCode() : 0);
        result = 31 * result + (_data != null ? _data.hashCode() : 0);
        result = 31 * result + (_position != null ? _position.hashCode() : 0);
        return result;
    }
}
