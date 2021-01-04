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
package com.gigaspaces.internal.server.space.executors;

import com.gigaspaces.internal.server.space.SpaceImpl;
import com.gigaspaces.internal.space.requests.BroadcastTableSpaceRequestInfo;
import com.gigaspaces.internal.space.requests.PushBroadcastTableEntriesSpaceRequestInfo;
import com.gigaspaces.internal.space.requests.PushBroadcastTableEntrySpaceRequestInfo;
import com.gigaspaces.internal.space.requests.SpaceRequestInfo;
import com.gigaspaces.internal.space.responses.BroadcastTableSpaceResponseInfo;
import com.gigaspaces.internal.space.responses.SpaceResponseInfo;
import com.gigaspaces.security.authorities.SpaceAuthority;
import com.j_spaces.core.UnknownTypeException;
import com.j_spaces.core.UnknownTypesException;
import com.j_spaces.core.client.Modifiers;
import net.jini.core.entry.UnusableEntryException;
import net.jini.core.transaction.TransactionException;

import java.rmi.RemoteException;

import static com.gigaspaces.internal.space.requests.BroadcastTableSpaceRequestInfo.Action.PUSH_ENTRIES;
import static com.gigaspaces.internal.space.requests.BroadcastTableSpaceRequestInfo.Action.PUSH_ENTRY;

public class SpaceBroadcastTableExecutor extends SpaceActionExecutor {
    @Override
    public SpaceResponseInfo execute(SpaceImpl space, SpaceRequestInfo spaceRequestInfo) {
        BroadcastTableSpaceResponseInfo responseInfo = new BroadcastTableSpaceResponseInfo();
        int partitionId = space.getPartitionId();
        if(partitionId == 0)
            return responseInfo;
        BroadcastTableSpaceRequestInfo requestInfo = (BroadcastTableSpaceRequestInfo) spaceRequestInfo;
        if(requestInfo.getAction() == PUSH_ENTRY) {
            PushBroadcastTableEntrySpaceRequestInfo info = (PushBroadcastTableEntrySpaceRequestInfo) spaceRequestInfo;
            try {
                if(info.isUpdate())
                    space.update(info.getEntry(), null, info.getLease(), info.getTimeout(), spaceRequestInfo.getSpaceContext(), Modifiers.add(info.getModifiers(), Modifiers.BACKUP_ONLY), true);
                else
                    space.write(info.getEntry(), null, info.getLease(), Modifiers.add(info.getModifiers(), Modifiers.BACKUP_ONLY), false, requestInfo.getSpaceContext());
            } catch (TransactionException | UnusableEntryException | UnknownTypeException | RemoteException | InterruptedException e) {
                responseInfo.addException(partitionId, e);
            }
        }
        if(requestInfo.getAction() == PUSH_ENTRIES) {
            PushBroadcastTableEntriesSpaceRequestInfo info = (PushBroadcastTableEntriesSpaceRequestInfo) spaceRequestInfo;
            try {
                space.write(info.getEntryPackets(), null, info.getLease(), info.getLeases(), info.getSpaceContext(), info.getTimeout(), Modifiers.add(info.getModifiers(), Modifiers.BACKUP_ONLY), true);
            } catch (TransactionException | UnknownTypesException | RemoteException e) {
                responseInfo.addException(partitionId, e);
            }
        }
        return responseInfo;
    }

    @Override
    public SpaceAuthority.SpacePrivilege getPrivilege() {
        return SpaceAuthority.SpacePrivilege.WRITE;
    }
}
