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

package com.gigaspaces.internal.space.requests;

import com.gigaspaces.async.AsyncResult;
import com.gigaspaces.events.NotifyInfo;
import com.gigaspaces.internal.cluster.node.impl.router.RouterStubHolder;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.space.responses.RegisterReplicationNotificationResponseInfo;
import com.gigaspaces.internal.transport.ITemplatePacket;
import com.gigaspaces.internal.version.PlatformLogicalVersion;
import com.gigaspaces.lrmi.LRMIInvocationContext;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;

/**
 * @author Dan Kilman
 * @since 9.0
 */
@com.gigaspaces.api.InternalApi
public class RegisterReplicationNotificationRequestInfo extends AbstractSpaceRequestInfo {
    private static final long serialVersionUID = 1L;

    public ITemplatePacket template;
    public RouterStubHolder viewStub;
    public NotifyInfo notifyInfo;
    public long eventId;
    public String tag;

    /**
     * Required for Externalizable
     */
    public RegisterReplicationNotificationRequestInfo() {
    }

    @Override
    public void writeExternal(ObjectOutput out)
            throws IOException {
        super.writeExternal(out);

        IOUtils.writeObject(out, template);
        IOUtils.writeObject(out, viewStub);
        IOUtils.writeObject(out, notifyInfo);
        out.writeLong(eventId);

        if (LRMIInvocationContext.getEndpointLogicalVersion().greaterOrEquals(PlatformLogicalVersion.v14_5_0)) {
            IOUtils.writeString(out, tag);
        }

    }

    @Override
    public void readExternal(ObjectInput in)
            throws IOException, ClassNotFoundException {
        super.readExternal(in);

        this.template = IOUtils.readObject(in);
        this.viewStub = IOUtils.readObject(in);
        this.notifyInfo = IOUtils.readObject(in);
        this.eventId = in.readLong();

        if (LRMIInvocationContext.getEndpointLogicalVersion().greaterOrEquals(PlatformLogicalVersion.v14_5_0)) {
            this.tag = IOUtils.readString(in);
        }
    }

    public RegisterReplicationNotificationResponseInfo reduce(List<AsyncResult<RegisterReplicationNotificationResponseInfo>> results)
            throws Exception {
        for (AsyncResult<RegisterReplicationNotificationResponseInfo> result : results) {
            if (result.getException() != null)
                throw result.getException();
            RegisterReplicationNotificationResponseInfo responseInfo = result.getResult();
            if (responseInfo.exception != null)
                throw responseInfo.exception;
        }

        return null;
    }
}
