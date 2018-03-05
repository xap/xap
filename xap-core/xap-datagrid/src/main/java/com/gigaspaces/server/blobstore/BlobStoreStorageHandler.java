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

import com.gigaspaces.datasource.DataIterator;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;


/**
 * basic methods for handling  blob-stored  objects specific implementations (SSD, off-heap buffers)
 * should extend this class
 *
 * @author yechielf
 * @since 10.0
 */
public abstract class BlobStoreStorageHandler {

    /**
     * initialize a blob-store implementation.
     *
     * @param blobStoreConfig - Configuration for blobstore implementation.
     */
    public void initialize(BlobStoreConfig blobStoreConfig) {
    }

    /**
     * Add the specified id with the specified data . Neither the id nor the data can be null.
     *
     * @param id         id with which the specified value is to be associated
     * @param data       data to be associated with the specified id
     * @param objectType the object type - one of  BlobStoreObjectType values
     * @return an optional position object which will be used as a pointer of locating the id, if
     * the operation succeeded, or null if irrelevant
     * @throws com.gigaspaces.server.blobstore.BlobStoreException if the specified id already exist
     *                                                            or container does not exist or
     *                                                            other failure.
     */
    public abstract Object add(java.io.Serializable id, java.io.Serializable data, BlobStoreObjectType objectType);

    /**
     * Returns the data to which the specified id is mapped,
     *
     * @param id         id with which the specified data is to be associated
     * @param position   optional position object which will be used as a pointer of locating the
     *                   id, or null if irrelevant
     * @param objectType the object type - one of  BlobStoreObjectType values
     * @return the data to which the specified id is mapped,
     * @throws com.gigaspaces.server.blobstore.BlobStoreException if the specified id does not
     *                                                            exist.
     */
    public abstract java.io.Serializable get(java.io.Serializable id, Object position, BlobStoreObjectType objectType);

    /**
     * Replace the data to which the specified id is mapped with new data,
     *
     * @param id         id with which the specified data is to be associated
     * @param data       the new data to replace the existing one.
     * @param position   optional position object which will be used as a pointer of locating the
     *                   id, or null if irrelevant
     * @param objectType the object type - one of  BlobStoreObjectType values
     * @return the new postion - an optional position object which will be used as a pointer of
     * locating the id, or null if irrelevant
     * @throws com.gigaspaces.server.blobstore.BlobStoreException if the specified id does not
     *                                                            exist.
     */
    public abstract Object replace(java.io.Serializable id, java.io.Serializable data, Object position, BlobStoreObjectType objectType);

    /**
     * Removes the id (and its corresponding data) from this FDF.
     *
     * @param id         the key that needs to be removed
     * @param position   optional position object which will be used as a pointer of locating the
     *                   id, or null if irrelevant
     * @param objectType the object type - one of  BlobStoreObjectType values
     * @return the data to which the specified id is mapped,
     * @throws com.gigaspaces.server.blobstore.BlobStoreException if the id does not exist.
     */
    public abstract java.io.Serializable remove(java.io.Serializable id, Object position, BlobStoreObjectType objectType);

    /**
     * . execute a bulk of operations on the blobstore. return a corresoping list of results note- a
     * non-abstract method, if not implemented the individual operations will be executed one by
     * one
     *
     * @param operations    - a list of operations to perform
     * @param objectType    the object type - one of  BlobStoreObjectType values
     * @param transactional - true if the bulk is to be performed in the blobstore as one logical
     *                      transaction if the blobstore supports transactions,  false otherwise
     * @return a list of results which corresponds to the input list of operations. if one or more
     * operations fail the returned result element will contain exception . if transactional
     * parameter is true a general exception should be thrown instead of a returned result element
     * with exception data
     */
    public List<BlobStoreBulkOperationResult> executeBulk(List<BlobStoreBulkOperationRequest> operations, BlobStoreObjectType objectType, boolean transactional) {
        List<BlobStoreBulkOperationResult> result = new ArrayList<BlobStoreBulkOperationResult>(operations.size());
        for (BlobStoreBulkOperationRequest request : operations) {
            if (request.getOpType() == BlobStoreBulkOperationType.ADD) {
                try {
                    result.add(new BlobStoreAddBulkOperationResult(request.getId(), add(request.getId(), request.getData(), objectType)));
                } catch (Exception ex) {
                    result.add(new BlobStoreAddBulkOperationResult(request.getId(), ex));
                }
            } else if (request.getOpType() == BlobStoreBulkOperationType.REMOVE) {
                try {
                    remove(request.getId(), request.getPosition(), objectType);
                    result.add(new BlobStoreRemoveBulkOperationResult(request.getId()));
                } catch (Exception ex) {
                    result.add(new BlobStoreRemoveBulkOperationResult(request.getId(), ex));
                }
            } else if (request.getOpType() == BlobStoreBulkOperationType.REPLACE) {
                try {
                    result.add(new BlobStoreReplaceBulkOperationResult(request.getId(), replace(request.getId(), request.getData(), request.getPosition(), objectType), request.getOffHeapInfo()));
                } catch (Exception ex) {
                    result.add(new BlobStoreReplaceBulkOperationResult(request.getId(), ex));
                }
            } else if (request.getOpType() == BlobStoreBulkOperationType.GET) {
                try {
                    result.add(new BlobStoreGetBulkOperationResult(request.getId(), get(request.getId(), request.getPosition(), objectType), request.getPosition(), request.getOffHeapInfo()));
                } catch (Exception ex) {
                    result.add(new BlobStoreGetBulkOperationResult(request.getId(), ex));
                }
            }
        }
        return result;
    }


    /**
     * . creates an iterator over the blobstore objeccts. note- a non-abstract method, if not
     * implemented null will be returned
     *
     * @param objectType the object type - one of  BlobStoreObjectType values
     * @return an iterator over the blobstore objects which belong to the requested objectType.
     * @throws com.gigaspaces.server.blobstore.BlobStoreException in case of a problenm.
     */
    public DataIterator<BlobStoreGetBulkOperationResult> iterator(BlobStoreObjectType objectType) {
        return null;
    }

    /**
     * . returns the properties passed (and also optionally set) by the blobstore handler.
     *
     * @return properties.
     */
    public Properties getProperties() {
        return null;
    }

    /**
     * . close the blobstore handler. note- a non-abstract method
     */
    public void close() {
    }
}

