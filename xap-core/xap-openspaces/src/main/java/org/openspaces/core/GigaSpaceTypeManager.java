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

package org.openspaces.core;

import com.gigaspaces.api.ExperimentalApi;
import com.gigaspaces.async.AsyncFuture;
import com.gigaspaces.async.AsyncFutureListener;
import com.gigaspaces.metadata.SpaceTypeDescriptor;
import com.gigaspaces.metadata.index.AddTypeIndexesResult;
import com.gigaspaces.metadata.index.SpaceIndex;

/**
 * Interface encapsulating operations for getting and managing space type descriptors.
 *
 * Use {@link GigaSpace#getTypeManager()} to retrieve the type manager of a <code>GigaSpace</code>
 * instance.
 *
 * @author Niv Ingberg
 * @see org.openspaces.core.GigaSpace
 * @see com.gigaspaces.metadata.SpaceTypeDescriptor
 * @see com.gigaspaces.metadata.SpaceTypeDescriptorBuilder
 * @see com.gigaspaces.metadata.index.SpaceIndex
 * @see com.gigaspaces.metadata.index.SpaceIndexFactory
 * @since 8.0
 */
public interface GigaSpaceTypeManager {

    /**
     * Gets the space type descriptor of the specified type.
     *
     * @param typeName Name of type.
     * @return Type descriptor of the type, if available (if not, returns null).
     */
    SpaceTypeDescriptor getTypeDescriptor(String typeName);

    /**
     * Gets the space type descriptor of the specified type.
     *
     * @param type Java class.
     * @return Type descriptor of the type, if available (if not, returns null).
     */
    SpaceTypeDescriptor getTypeDescriptor(Class<?> type);

    /**
     * Registers the specified space type descriptor in the space.
     */
    void registerTypeDescriptor(SpaceTypeDescriptor typeDescriptor);

    /**
     * Creates a type descriptor for the specified type and registers it in the space.
     */
    void registerTypeDescriptor(Class<?> type);

    /**
     * Unregisters the specified type from the space.
     * All entries of that type will be removed from the space.
     * All notify listeners of that type will be removed from the space without dispatching notifications.
     * <p>Note: This API is experimental with the following limitations, and possibly more:
     * <ol>
     * <li> <b>Concrete types:</b> When a concrete type (POJO, scala case class, etc.) is unregistered, its java class is
     * not unloaded from the server. This means you cannot register a modified version of that type.</li>
     * <li> <b>Subtypes:</b> A type with subtypes cannot be unregistered - you must first explicitly unregister all
     * subtypes</li>
     * <li> <b>High availability:</b> This operation is not replicated. As a workaround, you should demote
     * all primary instances and unregister the type again, to purge it from all instances.</li>
     * <li> <b>Mirror: </b> This operation is not replicated. As a workaround you should manually remove
     * the type from the underlying data store.</li>
     * <li> <b>WAN Gateway:</b>> This operation is not replicated. As a workaround you should remove
     * the type from the target cluster(s).</li>
     * <li> <b>memoryXtend:</b> TODO (off-heap? persistent?)</li>
     * <li> <b>Atomicity:</b> This operation is not carried out atomically across the cluster. Consider using quiesce for atomicity</li>
     * </ol>
     * </p>
     *
     * @param typeName Name of type to unregister
     * @since 15.5.1
     */
    //@ExperimentalApi
    //void unregisterTypeDescriptor(String typeName);

    /**
     * Adds the specified index to the specified type.
     *
     * @param typeName Name of type to enhance.
     * @param index    Index to add.
     * @return A Future to monitor completion of the operation, whose <code>get()</code> method will
     * return the add index result upon completion.
     */
    AsyncFuture<AddTypeIndexesResult> asyncAddIndex(String typeName, SpaceIndex index);

    /**
     * Adds the specified index to the specified type.
     *
     * @param typeName Name of type to enhance.
     * @param index    Index to add.
     * @param listener A listener to be notified when a result arrives
     * @return A Future to monitor completion of the operation, whose <code>get()</code> method will
     * return the add index result upon completion.
     */
    AsyncFuture<AddTypeIndexesResult> asyncAddIndex(String typeName, SpaceIndex index, AsyncFutureListener<AddTypeIndexesResult> listener);

    /**
     * Adds the specified indexes to the specified type.
     *
     * @param typeName Name of type to enhance.
     * @param indexes  Indexes to add.
     * @param listener A listener to be notified when a result arrives.
     * @return A Future to monitor completion of the operation, whose <code>get()</code> method will
     * return the add index result upon completion.
     */
    AsyncFuture<AddTypeIndexesResult> asyncAddIndexes(String typeName, SpaceIndex[] indexes, AsyncFutureListener<AddTypeIndexesResult> listener);
}
