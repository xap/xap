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

import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.server.space.SpaceImpl;
import com.gigaspaces.internal.space.requests.*;
import com.gigaspaces.internal.space.responses.BroadcastTableSpaceResponseInfo;
import com.gigaspaces.internal.space.responses.SpaceResponseInfo;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.internal.transport.TemplatePacket;
import com.gigaspaces.security.authorities.SpaceAuthority;
import com.j_spaces.core.SpaceContext;
import com.j_spaces.core.UnknownTypeException;
import com.j_spaces.core.UnknownTypesException;
import com.j_spaces.core.client.Modifiers;
import net.jini.core.entry.UnusableEntryException;
import net.jini.core.transaction.TransactionException;

import java.rmi.RemoteException;

import static com.gigaspaces.internal.space.requests.BroadcastTableSpaceRequestInfo.Action.*;
/**
 * @author alon shoham
 * @since 15.8.0
 */
@com.gigaspaces.api.InternalApi
public class SpaceBroadcastTableExecutor extends SpaceActionExecutor {
    @Override
    public SpaceResponseInfo execute(SpaceImpl space, SpaceRequestInfo spaceRequestInfo) {
        BroadcastTableSpaceResponseInfo responseInfo = new BroadcastTableSpaceResponseInfo();
        int partitionId = space.getPartitionId();
        BroadcastTableSpaceRequestInfo requestInfo = (BroadcastTableSpaceRequestInfo) spaceRequestInfo;
        if(partitionId == 0 && !requestInfo.getAction().equals(PULL_ENTRIES))
            return responseInfo;
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
                space.write(info.getEntryPackets(), null, info.getLease(), info.getLeases(), spaceRequestInfo.getSpaceContext(), info.getTimeout(), Modifiers.add(info.getModifiers(), Modifiers.BACKUP_ONLY), true);
            } catch (TransactionException | UnknownTypesException | RemoteException e) {
                responseInfo.addException(partitionId, e);
            }
        }
        if(requestInfo.getAction() == PULL_ENTRIES) {
            PullBroadcastTableEntriesSpaceRequestInfo info = (PullBroadcastTableEntriesSpaceRequestInfo) spaceRequestInfo;
            ITypeDesc typeDesc = space.getEngine().getTypeManager().getTypeDesc(info.getTypeName());
            if(typeDesc != null && typeDesc.isBroadcast()) {
                TemplatePacket templatePacket = new TemplatePacket(typeDesc);
                    try {
                    IEntryPacket[] entries = space.readMultiple(templatePacket, null, false, Integer.MAX_VALUE, spaceRequestInfo.getSpaceContext(), false, Modifiers.NONE);
                    if(entries != null && entries.length > 0)
                        responseInfo.setEntries(entries);
                    } catch (TransactionException | UnusableEntryException | UnknownTypeException | RemoteException e) {
                        responseInfo.addException(partitionId, e);
                    }
                }
            return responseInfo;
        }
        if(requestInfo.getAction() == CLEAR_ENTRIES) {
            ClearBroadcastTableEntriesSpaceRequestInfo info = (ClearBroadcastTableEntriesSpaceRequestInfo) spaceRequestInfo;
            try {
                space.clear(info.getTemplatePacket(), null, Modifiers.add(info.getModifiers(), Modifiers.BACKUP_ONLY), new SpaceContext());
            } catch (UnusableEntryException | UnknownTypeException | TransactionException | RemoteException e) {
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
