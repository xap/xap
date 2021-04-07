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

package com.gigaspaces.client.iterator.internal.tiered_storage;

import com.gigaspaces.client.iterator.ISpaceIteratorResult;
import com.gigaspaces.client.iterator.internal.ISpaceIteratorAggregatorPartitionResult;
import com.gigaspaces.internal.client.QueryResultTypeInternal;
import com.gigaspaces.internal.client.spaceproxy.ISpaceProxy;
import com.gigaspaces.internal.client.spaceproxy.SpaceProxyImpl;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.internal.transport.TemplatePacketFactory;
import com.gigaspaces.internal.utils.CollectionUtils;
import com.j_spaces.core.UidQueryPacket;

import java.util.*;

/**
 * @author Yael nahon
 * @since 16.0.0
 */
@com.gigaspaces.api.InternalApi
public class TieredSpaceIteratorResult implements ISpaceIteratorResult<TieredSpaceIteratorAggregatorPartitionResult> {

    private final List<IEntryPacket> entries = new ArrayList<IEntryPacket>();
    private final Map<Integer, Map<String, List<String>>> partitionedUids = new HashMap<>();


    public List<IEntryPacket> getEntries() {
        return entries;
    }

    @Override
    public void addPartition(TieredSpaceIteratorAggregatorPartitionResult partitionResult) {
        entries.addAll(partitionResult.getEntries());
        if (partitionResult.getUids() != null) {
            partitionedUids.put(partitionResult.getPartitionId(), partitionResult.getUids());
        }
    }

    public void close() {
        entries.clear();
        partitionedUids.clear();
    }

    public UidQueryPacket buildQueryPacket(ISpaceProxy spaceProxy, int batchSize, QueryResultTypeInternal resultType) {
        final Integer partitionId = CollectionUtils.first(partitionedUids.keySet());
        if (partitionId == null)
            return null;

        Map<String, List<String>> partitionUids = partitionedUids.get(partitionId);
        String nextType = partitionUids.keySet().iterator().next();
        List<String> uids = partitionUids.get(nextType);
        final String[] batch = new String[Math.min(batchSize, uids.size())];
        final Iterator<String> iterator = uids.iterator();
        int index = 0;
        while (iterator.hasNext() && index < batch.length) {
            batch[index++] = iterator.next();
            iterator.remove();
        }
        if (uids.isEmpty()) {
            partitionedUids.get(partitionId).remove(nextType);
            if (partitionedUids.get(partitionId).isEmpty()) {
                partitionedUids.remove(partitionId);
            }
        }

        ITypeDesc typeDesc = ((SpaceProxyImpl) spaceProxy).getTypeManager().getTypeDescByName(nextType);
        UidQueryPacket queryPacket = (UidQueryPacket) TemplatePacketFactory.createUidsPacket(typeDesc, batch, resultType, false);
        queryPacket.setRouting(partitionId);
        return queryPacket;
    }

    @Override
    public int size() {
        int size = entries.size();
        for (Map<String, List<String>> partitionResult : partitionedUids.values()) {
            for (List<String> typeResult : partitionResult.values()) {
                size += typeResult.size();
            }
        }
        return size;
    }
}
