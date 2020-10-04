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

package com.gigaspaces.internal.query;

import com.gigaspaces.internal.server.storage.IEntryData;
import com.gigaspaces.internal.server.storage.ITemplateHolder;
import com.gigaspaces.internal.transport.EntryPacketFactory;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.query.aggregators.SpaceEntriesAggregator;
import com.gigaspaces.query.aggregators.SpaceEntriesAggregatorContext;
import com.gigaspaces.server.ServerEntry;
import com.j_spaces.core.cache.context.Context;

import java.util.List;

/**
 * @author Niv Ingberg
 * @since 10.0
 */
@com.gigaspaces.api.InternalApi
public class EntryHolderAggregatorContext extends SpaceEntriesAggregatorContext {

    private final ITemplateHolder template;
    private final int partitionId;
    private IEntryData entryData;
    private String uid;
    private boolean isTransient;

    public EntryHolderAggregatorContext(List<SpaceEntriesAggregator> aggregators, ITemplateHolder template,
                                        int partitionId) {
        super(aggregators);
        this.template = template;
        this.partitionId = partitionId;
    }

    public void scan(Context context, IEntryData entryData, String uid, boolean isTransient) {
        this.entryData = context.getViewEntryData(entryData);
        this.uid = uid;
        this.isTransient = isTransient;
        aggregate();
    }

    @Override
    public int getPartitionId() {
        return partitionId;
    }

    @Override
    public String getEntryUid() {
        return uid;
    }

    @Override
    public RawEntry getRawEntry() {
        return EntryPacketFactory.createFullPacket(null, template, entryData, uid, isTransient);
    }


    @Override
    public void applyProjectionTemplate(RawEntry entry) {
        if (template.getProjectionTemplate() != null)
            template.getProjectionTemplate().filterOutNonProjectionProperties((IEntryPacket) entry);
    }

    @Override
    public ServerEntry getServerEntry() {
        return entryData;
    }
}
