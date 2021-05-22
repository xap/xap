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

package com.gigaspaces.internal.cluster.node.impl.backlog.multisourcesinglefile;

import com.gigaspaces.internal.cluster.node.impl.backlog.reliableasync.IReliableAsyncState;
import com.gigaspaces.internal.cluster.node.impl.backlog.reliableasync.IReliableAsyncTargetState;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.serialization.SmartExternalizable;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * @author idan
 * @since 8.0.4
 */
@com.gigaspaces.api.InternalApi
public class MultiSourceSingleFileReliableAsyncState
        implements IReliableAsyncState {
    private static final long serialVersionUID = 1L;
    private MultiSourceAsyncTargetState[] _asyncTargetsState;

    public MultiSourceSingleFileReliableAsyncState() {
    }

    public MultiSourceSingleFileReliableAsyncState(MultiSourceAsyncTargetState[] asyncTargetState) {
        _asyncTargetsState = asyncTargetState;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        IOUtils.writeObject(out, _asyncTargetsState);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        _asyncTargetsState = IOUtils.readObject(in);
    }

    public MultiSourceAsyncTargetState[] getAsyncTargetsState() {
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
        for (MultiSourceAsyncTargetState asyncTargetState : _asyncTargetsState) {
            if (!asyncTargetState.hadAnyHandshake())
                return 0;
            minConfirmed = Math.min(minConfirmed, asyncTargetState.getLastConfirmedKey());
        }
        return minConfirmed + 1;
    }

    public static class MultiSourceAsyncTargetState implements SmartExternalizable, IReliableAsyncTargetState {
        private static final long serialVersionUID = 1L;

        private String _targetMemberName;
        private long _lastConfirmedKey = -1;
        private long _lastReceivedKey = -1;
        private boolean _hadAnyHandshake = false;

        public MultiSourceAsyncTargetState() {
        }

        public MultiSourceAsyncTargetState(String targetMemberName, MultiSourceSingleFileConfirmationHolder confirmationHolder) {
            _targetMemberName = targetMemberName;
            _lastConfirmedKey = confirmationHolder.getLastConfirmedKey();
            _lastReceivedKey = confirmationHolder.getLastReceivedKey();
            _hadAnyHandshake = confirmationHolder.hadAnyHandshake();
        }

        public long getLastConfirmedKey() {
            return _lastConfirmedKey;
        }

        public long getLastReceivedKey() {
            return _lastReceivedKey;
        }

        @Override
        public void writeExternal(ObjectOutput out) throws IOException {
            IOUtils.writeRepetitiveString(out, _targetMemberName);
            out.writeLong(_lastConfirmedKey);
            out.writeLong(_lastReceivedKey);
            out.writeBoolean(_hadAnyHandshake);
        }

        @Override
        public void readExternal(ObjectInput in) throws IOException,
                ClassNotFoundException {
            _targetMemberName = IOUtils.readRepetitiveString(in);
            _lastConfirmedKey = in.readLong();
            _lastReceivedKey = in.readLong();
            _hadAnyHandshake = in.readBoolean();
        }

        public boolean hadAnyHandshake() {
            return _hadAnyHandshake;
        }

        @Override
        public String getTargetMemberName() {
            return _targetMemberName;
        }

        @Override
        public String toString() {
            return "MultiSourceAsyncTargetState [targetMemberName="
                    + _targetMemberName + ", lastConfirmedKey="
                    + _lastConfirmedKey + ", lastReceivedKey="
                    + _lastReceivedKey + ", hadAnyHandshake="
                    + _hadAnyHandshake + "]";
        }
    }

}
