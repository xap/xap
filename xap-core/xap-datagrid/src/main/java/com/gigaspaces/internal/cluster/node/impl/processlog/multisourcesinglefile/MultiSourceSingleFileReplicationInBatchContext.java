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

package com.gigaspaces.internal.cluster.node.impl.processlog.multisourcesinglefile;

import com.gigaspaces.internal.cluster.node.impl.AbstractReplicationInBatchContext;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author idan
 * @author eitan
 * @since 8.0.4
 */
@com.gigaspaces.api.InternalApi
public class MultiSourceSingleFileReplicationInBatchContext
        extends AbstractReplicationInBatchContext {
    private final IReplicationParticipantsMediator _participantsMediator;
    private final IPacketConsumedCallback _packetConsumedCallback;
    private List<MultiSourceProcessLogPacket> _beingConsumedPackets;

    public MultiSourceSingleFileReplicationInBatchContext(
            IPacketConsumedCallback packetConsumedCallback, Logger contextLogger,
            int entireBatchSize, IReplicationParticipantsMediator participantsMediator, String sourceLookupName, String groupName) {
        super(contextLogger, entireBatchSize, sourceLookupName, groupName, true);
        _packetConsumedCallback = packetConsumedCallback;
        _participantsMediator = participantsMediator;
        _beingConsumedPackets = new LinkedList<MultiSourceProcessLogPacket>();
    }

    public void addBeingConsumedPacket(MultiSourceProcessLogPacket packet) {
        if (_beingConsumedPackets == null)
            _beingConsumedPackets = new LinkedList<MultiSourceProcessLogPacket>();
        _beingConsumedPackets.add(packet);
    }

    private MultiSourceProcessLogPacket removeLastConsumedPacket() {
        if (!_beingConsumedPackets.isEmpty())
            return _beingConsumedPackets.remove(_beingConsumedPackets.size() - 1);
        return null;
    }

    @Override
    protected void afterBatchConsumed(long lastProcessedKeyInBatch) {
        if (_beingConsumedPackets != null && !_beingConsumedPackets.isEmpty()) {
            for (Iterator<MultiSourceProcessLogPacket> iterator = _beingConsumedPackets.iterator(); iterator.hasNext(); ) {
                final MultiSourceProcessLogPacket consumedPacket = iterator.next();
                // Verify packet is in the current consumption context
                if (consumedPacket.getKey() <= lastProcessedKeyInBatch) {
                    if (consumedPacket.isMultiParticipant()) {
                        if (consumedPacket.isConsolidatedByThisParticipant())
                            _participantsMediator.setConsumed(consumedPacket.getParticipantsMetadata());
                        else
                            _participantsMediator.remove(consumedPacket.getParticipantsMetadata());
                    }
                    consumedPacket.setConsumed();
                    _packetConsumedCallback.packetConsumed(consumedPacket.getKey());
                    iterator.remove();
                }
            }
        }
    }

    @Override
    public void rollback() {
        // Roll back is relevant for the last added packet to the batch context.
        // therefore we set this packet as unconsumed.
        MultiSourceProcessLogPacket packet = removeLastConsumedPacket();
        if (packet != null)
            packet.setUnconsumed();
        super.rollback();
    }

    @Override
    public boolean isBatchContext() {
        return true;
    }

    public List<MultiSourceProcessLogPacket> getBeingConsumedPackets() {
        return _beingConsumedPackets;
    }

}
