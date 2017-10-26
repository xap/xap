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

package com.j_spaces.core.cache.blobStore.storage.bulks.delayedReplication;

/**
 * public void handleUpdateEntryReplication(Context context,IEntryHolder new_eh,IEntryData
 * originalData, Collection<SpaceEntryMutator> mutators)
 *
 * Created by yechielf on 08/12/2015.
 */

import com.gigaspaces.client.mutators.SpaceEntryMutator;
import com.gigaspaces.internal.server.storage.IEntryData;
import com.gigaspaces.internal.server.storage.IEntryHolder;

import java.util.Collection;

/**
 * used in blobstore bulks + embedded list in order to perform replication after flush
 *
 * @author yechielf
 * @since 11.0
 */

@com.gigaspaces.api.InternalApi
public class DelayedReplicationUpdateInfo extends DelayedReplicationBasicInfo {


    private final IEntryData _originalData;
    private final Collection<SpaceEntryMutator> _mutators;

    public DelayedReplicationUpdateInfo(IEntryHolder new_eh, IEntryData originalData, Collection<SpaceEntryMutator> mutators) {
        super(new_eh, DelayedReplicationOpCodes.UPDATE);
        _originalData = originalData;
        _mutators = mutators;
    }

    public IEntryData getOriginalData() {
        return _originalData;
    }

    public Collection<SpaceEntryMutator> getMutators() {
        return _mutators;
    }

}
