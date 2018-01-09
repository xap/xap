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

import com.gigaspaces.internal.cluster.node.impl.backlog.reliableasync.IReliableAsyncState;
import com.gigaspaces.internal.cluster.node.impl.backlog.sync.IMarker;
import com.gigaspaces.internal.cluster.node.impl.packets.IReplicationOrderedPacket;
import com.gigaspaces.internal.cluster.node.impl.processlog.IProcessLogHandshakeResponse;

/**
 * @author eitany
 * @since 8.0.5
 */
public interface IReplicationReliableAsyncMediator {

    void reliableAsyncSourceAdd(String sourceLookupName,
                                IReplicationOrderedPacket packet);

    void reliableAsyncSourceKeep(String sourceMemberName,
                                 IReplicationOrderedPacket packet);

    void afterHandshake(IProcessLogHandshakeResponse handshakeResponse);

    void updateReliableAsyncState(IReliableAsyncState reliableAsyncState, String sourceMemberName);

    IMarker getMarker(IReplicationOrderedPacket packet, String membersGroupName);

    void performCompaction();


}
