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

package com.gigaspaces.internal.client.spaceproxy.actioninfo;

import com.gigaspaces.client.ReadMultipleException;
import com.gigaspaces.internal.client.spaceproxy.ISpaceProxy;
import com.gigaspaces.internal.transport.AbstractProjectionTemplate;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.metadata.SpaceMetadataException;
import com.j_spaces.core.client.ReadModifiers;

import java.util.UUID;

/**
 * @author Alon Shoham
 * @since 15.2.0
 */
@com.gigaspaces.api.InternalApi
public class GetBatchForIteratorProxyActionInfo extends QueryProxyActionInfo {
    public final UUID uuid;
    public final int batchSize;
    public final int batchNumber;

    public GetBatchForIteratorProxyActionInfo(ISpaceProxy spaceProxy, Object template, int batchSize, int batchNumber, int modifiers, UUID uuid) {
        super(spaceProxy, template, null, modifiers, false);
        try {
            // If query is a uids query, the maximum number of results is also limited by the uids length:
            if (queryPacket.getMultipleUIDs() != null) {
                this.batchSize = Math.min(batchSize, queryPacket.getMultipleUIDs().length);
            } else {
                this.batchSize = batchSize;
            }
            this.batchNumber = batchNumber;
            this.uuid = uuid;
            if (ReadModifiers.isFifoGroupingPoll(modifiers))
                verifyFifoGroupsCallParams(false);

            setFifoIfNeeded(spaceProxy);
        } catch (SpaceMetadataException e) {
                throw new ReadMultipleException(e);
        }
    }

    public Object[] convertQueryResults(ISpaceProxy spaceProxy, IEntryPacket[] results, AbstractProjectionTemplate projectionTemplate) {
        boolean returnPacket = _query == queryPacket;
        Object[] returnedObjects = spaceProxy.getDirectProxy().getTypeManager().convertQueryResults(results,
                queryPacket,
                returnPacket,
                projectionTemplate);
        return returnedObjects;
    }

    private void verifyFifoGroupsCallParams(boolean isTake) {
        if (txn == null)
            throw new IllegalArgumentException(" fifo-groups operation must be under transaction");
        if (!isTake && !(ReadModifiers.isExclusiveReadLock(modifiers)))
            throw new IllegalArgumentException(" fifo-groups read operation must be exclusive-read-lock");
    }
}
