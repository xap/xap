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

package com.gigaspaces.internal.cluster.node.impl.groups;

import com.gigaspaces.internal.cluster.node.impl.packets.data.IReplicationPacketEntryData;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IReplicationPacketEntryDataContentExtractor;
import com.gigaspaces.internal.version.PlatformLogicalVersion;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Dan Kilman
 * @since 9.0
 */
public abstract class AbstractReplicationChannelDataFilter
        implements IReplicationChannelDataFilter {

    @Override
    public void filterAfterReplicatedEntryData(
            IReplicationPacketEntryData data,
            PlatformLogicalVersion targetLogicalVersion,
            IReplicationPacketEntryDataContentExtractor contentExtractor,
            Logger contextLogger) {

    }

    @Override
    public boolean filterBeforeReplicatingEntryDataHasSideEffects() {
        return false;
    }

}
