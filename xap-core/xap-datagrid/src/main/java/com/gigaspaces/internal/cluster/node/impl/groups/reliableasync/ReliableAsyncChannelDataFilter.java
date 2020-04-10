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

package com.gigaspaces.internal.cluster.node.impl.groups.reliableasync;

import com.gigaspaces.internal.cluster.node.impl.ReplicationMultipleOperationType;
import com.gigaspaces.internal.cluster.node.impl.groups.AbstractReplicationChannelDataFilter;
import com.gigaspaces.internal.cluster.node.impl.groups.ReplicationChannelDataFilterResult;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IReplicationPacketData;
import com.gigaspaces.internal.version.PlatformLogicalVersion;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public abstract class ReliableAsyncChannelDataFilter extends AbstractReplicationChannelDataFilter {

    @Override
    public ReplicationChannelDataFilterResult filterBeforeReplicatingData(IReplicationPacketData<?> data, PlatformLogicalVersion targetLogicalVersion, Logger contextLogger) {
        switch (data.getMultipleOperationType()) {
            case TRANSACTION_ONE_PHASE:
                return ReplicationChannelDataFilterResult.PASS;
            case TRANSACTION_TWO_PHASE_PREPARE:
            case TRANSACTION_TWO_PHASE_ABORT:
                return ReplicationChannelDataFilterResult.FILTER_DATA;
            case TRANSACTION_TWO_PHASE_COMMIT:
                return ReplicationChannelDataFilterResult.getConvertToOperationResult(ReplicationMultipleOperationType.TRANSACTION_ONE_PHASE);
        }

        return ReplicationChannelDataFilterResult.FILTER_DATA;
    }

}
