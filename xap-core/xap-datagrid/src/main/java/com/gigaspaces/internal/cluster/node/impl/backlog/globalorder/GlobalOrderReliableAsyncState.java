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

package com.gigaspaces.internal.cluster.node.impl.backlog.globalorder;

import com.gigaspaces.internal.cluster.node.impl.backlog.reliableasync.IReliableAsyncState;
import com.gigaspaces.internal.cluster.node.impl.backlog.reliableasync.IReliableAsyncTargetState;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.serialization.SmartExternalizable;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;

@com.gigaspaces.api.InternalApi
public class GlobalOrderReliableAsyncState implements SmartExternalizable, IReliableAsyncState {
    private static final long serialVersionUID = 1L;

    private AsyncTargetState[] _asyncTargetsState;

    public GlobalOrderReliableAsyncState() {
    }

    public GlobalOrderReliableAsyncState(AsyncTargetState[] asyncTargetsState) {
        _asyncTargetsState = asyncTargetsState;
    }

    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        _asyncTargetsState = IOUtils.readObject(in);
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        IOUtils.writeObject(out, _asyncTargetsState);
    }

    public AsyncTargetState[] getAsyncTargetsState() {
        return _asyncTargetsState;
    }

    @Override
    public IReliableAsyncTargetState[] getReliableAsyncTargetsState() {
        return getAsyncTargetsState();
    }

    public long getMinimumUnconfirmedKey() {
        if (_asyncTargetsState.length == 0)
            return Long.MAX_VALUE;
        long minConfirmed = Long.MAX_VALUE;
        for (AsyncTargetState asyncTargetState : _asyncTargetsState) {
            if (!asyncTargetState.hadAnyHandshake())
                return 0;
            minConfirmed = Math.min(minConfirmed, asyncTargetState.getLastConfirmedKey());
        }
        return minConfirmed + 1;
    }

    @Override
    public String toString() {
        return Arrays.toString(_asyncTargetsState);
    }

    public static class AsyncTargetState implements SmartExternalizable, IReliableAsyncTargetState {
        private static final long serialVersionUID = 1L;

        private String _targetMemberName;
        private long _lastConfirmedKey = -1;
        private boolean _hadAnyHandshake = false;

        public AsyncTargetState() {
        }

        public AsyncTargetState(String targetMemberName, GlobalOrderConfirmationHolder confirmationHolder) {
            _targetMemberName = targetMemberName;
            _lastConfirmedKey = confirmationHolder.getLastConfirmedKey();
            _hadAnyHandshake = confirmationHolder.hadAnyHandshake();
        }

        public void readExternal(ObjectInput in) throws IOException,
                ClassNotFoundException {
            _targetMemberName = IOUtils.readRepetitiveString(in);
            _hadAnyHandshake = in.readBoolean();
            _lastConfirmedKey = in.readLong();
        }

        public void writeExternal(ObjectOutput out) throws IOException {
            IOUtils.writeRepetitiveString(out, _targetMemberName);
            out.writeBoolean(_hadAnyHandshake);
            out.writeLong(_lastConfirmedKey);
        }

        @Override
        public String getTargetMemberName() {
            return _targetMemberName;
        }

        public long getLastConfirmedKey() {
            return _lastConfirmedKey;
        }

        public boolean hadAnyHandshake() {
            return _hadAnyHandshake;
        }

        @Override
        public String toString() {
            return "AsyncTargetState [targetMemberName=" + _targetMemberName
                    + ", lastConfirmedKey=" + _lastConfirmedKey + ", hadAnyHandshake=" + _hadAnyHandshake + "]";
        }

    }

}
