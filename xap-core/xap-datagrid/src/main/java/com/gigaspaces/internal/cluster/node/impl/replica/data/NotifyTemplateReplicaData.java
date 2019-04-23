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

package com.gigaspaces.internal.cluster.node.impl.replica.data;

import com.gigaspaces.events.GSEventRegistration;
import com.gigaspaces.events.NotifyInfo;
import com.gigaspaces.internal.cluster.node.impl.IIncomingReplicationFacade;
import com.gigaspaces.internal.cluster.node.impl.replica.ISpaceReplicaConsumeFacade;
import com.gigaspaces.internal.cluster.node.impl.replica.SpaceCopyIntermediateResult;
import com.gigaspaces.internal.cluster.node.impl.replica.data.filters.NotifyReplicationFilterReplicaDataWrapper;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.server.space.metadata.SpaceTypeManager;
import com.gigaspaces.internal.transport.ITemplatePacket;
import com.j_spaces.core.cluster.IReplicationFilterEntry;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;


@com.gigaspaces.api.InternalApi
public class NotifyTemplateReplicaData
        extends AbstractReplicaData {
    private static final long serialVersionUID = 1L;

    private ITemplatePacket _templatePacket;

    private NotifyInfo _notifyInfo;
    private String _templateUid;
    private int _objectType;

    public NotifyTemplateReplicaData() {
    }

    public NotifyTemplateReplicaData(ITemplatePacket templatePacket, String templateUid,
                                     NotifyInfo notifyInfo, int objectType) {
        _templatePacket = templatePacket;
        _templateUid = templateUid;
        _notifyInfo = notifyInfo;
        _objectType = objectType;

    }

    public void writeExternal(ObjectOutput out) throws IOException {
        IOUtils.writeObject(out, _templatePacket);
        IOUtils.writeString(out, _templateUid);
        IOUtils.writeObject(out, _notifyInfo);
        out.writeInt(_objectType);
    }

    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        _templatePacket = IOUtils.readObject(in);
        _templateUid = IOUtils.readString(in);
        _notifyInfo = IOUtils.readObject(in);
        _objectType = in.readInt();
    }

    public String getUid() {
        return _templateUid;
    }

    public void execute(ISpaceReplicaConsumeFacade consumeFacade,
                        SpaceCopyIntermediateResult intermediateResult, IIncomingReplicationFacade incomingReplicationFacade) throws Exception {
        GSEventRegistration eventRegistration = consumeFacade.insertNotifyTemplate(_templatePacket, _templateUid, _notifyInfo);

        intermediateResult.addRegisteredNotifyTemplate(_templatePacket.getTypeName(), eventRegistration);
    }

    public boolean supportsReplicationFilter() {
        return true;
    }

    @Override
    public boolean isEntryReplicaData() {
        return false;
    }

    public IReplicationFilterEntry toFilterEntry(SpaceTypeManager typeManager) {
        ITypeDesc typeDesc = getTypeDescriptor(typeManager, _templatePacket);
        return new NotifyReplicationFilterReplicaDataWrapper(_templatePacket, _notifyInfo, _objectType, typeDesc);
    }

    @Override
    public String toString() {
        return "NotifyTemplateReplicaData, templateUid [" + _templateUid + "] " + _templatePacket;
    }
}
