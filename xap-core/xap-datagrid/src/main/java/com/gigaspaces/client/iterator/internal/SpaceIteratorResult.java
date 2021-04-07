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

package com.gigaspaces.client.iterator.internal;

import com.gigaspaces.client.iterator.ISpaceIteratorResult;
import com.gigaspaces.internal.client.QueryResultTypeInternal;
import com.gigaspaces.internal.client.spaceproxy.ISpaceProxy;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.internal.transport.TemplatePacketFactory;
import com.gigaspaces.internal.utils.CollectionUtils;
import com.j_spaces.core.UidQueryPacket;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.LinkedList;

/**
 * @author Niv Ingberg
 * @since 10.1
 */
@com.gigaspaces.api.InternalApi
public class SpaceIteratorResult implements ISpaceIteratorResult<SpaceIteratorAggregatorPartitionResult> {

    private final List<IEntryPacket> entries = new ArrayList<IEntryPacket>();
    private final Map<Integer, LinkedList<String>> partitionedUids = new HashMap<Integer, LinkedList<String>>();

    public void addPartition(SpaceIteratorAggregatorPartitionResult partitionResult) {
        entries.addAll(partitionResult.getEntries());
        if (partitionResult.getUids() != null) {
            partitionedUids.put(partitionResult.getPartitionId(), new LinkedList<String>(partitionResult.getUids()));
        }
    }

    public List<IEntryPacket> getEntries() {
        return entries;
    }

    public void close() {
        entries.clear();
        partitionedUids.clear();
    }

    public UidQueryPacket buildQueryPacket(int batchSize, QueryResultTypeInternal resultType) {
        final Integer partitionId = CollectionUtils.first(partitionedUids.keySet());
        if (partitionId == null)
            return null;

        final LinkedList<String> uids = partitionedUids.get(partitionId);
        final String[] batch = new String[Math.min(batchSize, uids.size())];
        final Iterator<String> iterator = uids.iterator();
        int index = 0;
        while (iterator.hasNext() && index < batch.length) {
            batch[index++] = iterator.next();
            iterator.remove();
        }
        if (uids.isEmpty())
            partitionedUids.remove(partitionId);

        UidQueryPacket queryPacket = (UidQueryPacket) TemplatePacketFactory.createUidsPacket(batch, resultType, false);
        queryPacket.setRouting(partitionId);
        return queryPacket;
    }

    @Override
    public int size() {
        int size = entries.size();
        for (LinkedList<String> uids : partitionedUids.values())
            size += uids.size();
        return size;
    }
}
