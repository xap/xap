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

package com.gigaspaces.internal.cluster.node.impl.backlog;

import com.gigaspaces.internal.cluster.node.impl.backlog.BacklogConfig.LimitReachedPolicy;
import com.gigaspaces.internal.version.PlatformLogicalVersion;
import com.gigaspaces.lrmi.LRMIInvocationContext;
import com.gigaspaces.serialization.SmartExternalizable;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

@com.gigaspaces.api.InternalApi
public class BacklogMemberLimitationConfig implements SmartExternalizable {

    private static final long serialVersionUID = 1L;

    private long _limit = BacklogConfig.UNLIMITED;
    private LimitReachedPolicy _limitReachedPolicy = LimitReachedPolicy.BLOCK_NEW;
    private long _limitDuringSynchronization = BacklogConfig.UNLIMITED;
    private LimitReachedPolicy _limitReachedPolicyDuringSynchronization = LimitReachedPolicy.BLOCK_NEW;

    public BacklogMemberLimitationConfig() {
    }

    public void setLimit(long limit, LimitReachedPolicy limitReachedPolicy) {
        _limit = limit;
        _limitReachedPolicy = limitReachedPolicy;
    }

    public void setUnlimited() {
        _limit = BacklogConfig.UNLIMITED;
    }

    public boolean isLimited() {
        return _limit != BacklogConfig.UNLIMITED;
    }

    public void setUnlimitedDuringSynchronization() {
        _limitDuringSynchronization = BacklogConfig.UNLIMITED;
    }

    public boolean isLimitedDuringSynchronization() {
        return _limitDuringSynchronization != BacklogConfig.UNLIMITED;
    }

    public void setLimitDuringSynchronization(long limit, LimitReachedPolicy limitReachedPolicy) {
        _limitDuringSynchronization = limit;
        _limitReachedPolicyDuringSynchronization = limitReachedPolicy;
    }

    public long getLimit() {
        return _limit;
    }

    public long getLimitDuringSynchronization() {
        return _limitDuringSynchronization;
    }

    public LimitReachedPolicy getLimitReachedPolicy() {
        return _limitReachedPolicy;
    }

    public LimitReachedPolicy getLimitReachedPolicyDuringSynchronization() {
        return _limitReachedPolicyDuringSynchronization;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(_limit);
        out.writeByte(_limitReachedPolicy.getCode());
        out.writeLong(_limitDuringSynchronization);
        if (LRMIInvocationContext.getEndpointLogicalVersion().greaterOrEquals(PlatformLogicalVersion.v9_7_0))
            out.writeByte(_limitReachedPolicyDuringSynchronization.getCode());

    }

    @Override
    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        _limit = in.readLong();
        _limitReachedPolicy = LimitReachedPolicy.fromCode(in.readByte());
        _limitDuringSynchronization = in.readLong();
        if (LRMIInvocationContext.getEndpointLogicalVersion().greaterOrEquals(PlatformLogicalVersion.v9_7_0))
            _limitReachedPolicyDuringSynchronization = LimitReachedPolicy.fromCode(in.readByte());
    }

    @Override
    public String toString() {
        return "BacklogMemberLimitationConfig [_limit=" + _limit
                + ", _limitReachedPolicy=" + _limitReachedPolicy
                + ", _limitDuringSynchronization="
                + _limitDuringSynchronization
                + ", _limitReachedPolicyDuringSynchronization="
                + _limitReachedPolicyDuringSynchronization + "]";
    }


}
