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

package com.gigaspaces.internal.transport;

import com.gigaspaces.internal.query.ICustomQuery;
import com.gigaspaces.internal.query.RawEntry;
import com.gigaspaces.internal.server.space.redolog.storage.bytebuffer.ISwapExternalizable;

/**
 * this is the basic interface defining the proxy-space or space-space transport layer.
 * implementations depends on type of the data being transported.
 *
 * @author asy ronen
 * @since 6.5
 */
public interface IEntryPacket extends ITransportPacket, ISwapExternalizable, RawEntry {
    void setUID(String uid);

    Object getID();

    long getTTL();

    void setTTL(long ttl);

    IEntryPacket clone();

    void setCustomQuery(ICustomQuery customQuery);

    /**
     * Sets the entry's previous version in space before update, relevant for replication update
     * operation
     */
    void setPreviousVersion(int version);

    /**
     * Gets the entry's previous version in space before update
     */
    int getPreviousVersion();

    /**
     * Gets whether this packet has a previous version set.
     */
    boolean hasPreviousVersion();

    /**
     * true if the entry packet has an array of fixed properties
     */
    boolean hasFixedPropertiesArray();

    //Temp until we remove externalizable entry packet completely
    boolean isExternalizableEntryPacket();

    default void setBinaryFields(byte[] binaryFields) {

    }

    default boolean allNullFieldValues() {
        return getFieldValues() == null;
    }

}
