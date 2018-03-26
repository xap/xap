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

package com.gigaspaces.internal.cluster.node.impl.packets.data.errors;

import com.gigaspaces.events.NotifyInfo;
import com.gigaspaces.internal.cluster.node.IReplicationInContext;
import com.gigaspaces.internal.cluster.node.impl.packets.data.AbstractDataConsumeFix;
import com.gigaspaces.internal.cluster.node.impl.packets.data.DiscardReplicationPacketData;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IDataConsumeFixFacade;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IExecutableReplicationPacketData;
import com.gigaspaces.internal.cluster.node.impl.packets.data.ReplicationPacketDataConsumer;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.transport.ITemplatePacket;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;


/**
 * Fix a problem of extend notify template lease that do not exists anymore in the target (long
 * disconnection ,recovery). This will resurrect the template and discard the extend lease
 * operation
 *
 * @author eitany
 * @since 8.0
 */
@com.gigaspaces.api.InternalApi
public class ResurrectNotifyTemplateFix
        extends AbstractDataConsumeFix {
    private static final long serialVersionUID = 1L;
    private ITemplatePacket _notifyTemplate;
    private String _uid;
    private NotifyInfo _notifyInfo;

    public ResurrectNotifyTemplateFix() {
    }

    public ResurrectNotifyTemplateFix(ITemplatePacket notifyTemplate,
                                      String uid, NotifyInfo notifyInfo) {
        _notifyTemplate = notifyTemplate;
        _uid = uid;
        _notifyInfo = notifyInfo;
    }

    @Override
    public IExecutableReplicationPacketData<?> fix(IReplicationInContext context,
                                                   IDataConsumeFixFacade fixFacade,
                                                   ReplicationPacketDataConsumer consumer, IExecutableReplicationPacketData<?> data) throws Exception {
        fixFacade.insertNotifyTemplate(_notifyTemplate, _uid, _notifyInfo);
        return DiscardReplicationPacketData.instance();
    }

    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        _notifyTemplate = IOUtils.readObject(in);
        _uid = IOUtils.readString(in);
        _notifyInfo = IOUtils.readObject(in);
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        IOUtils.writeObject(out, _notifyTemplate);
        IOUtils.writeString(out, _uid);
        IOUtils.writeObject(out, _notifyInfo);
    }

    @Override
    public String toString() {
        return "ResurrectNotifyTemplateFix - UID=" + _uid + " Template=" + _notifyTemplate;
    }

}
