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

package com.gigaspaces.internal.cluster.node.impl.backlog.multibucketsinglefile;

import com.gigaspaces.internal.cluster.node.impl.backlog.reliableasync.IReliableAsyncState;
import com.gigaspaces.internal.cluster.node.impl.backlog.reliableasync.IReliableAsyncTargetState;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.serialization.SmartExternalizable;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;

@com.gigaspaces.api.InternalApi
public class MultiBucketSingleFileReliableAsyncState
        implements IReliableAsyncState {

    private static final long serialVersionUID = 1L;
    private AsyncTargetState[] _asyncTargetsState;

    public MultiBucketSingleFileReliableAsyncState() {
    }

    public MultiBucketSingleFileReliableAsyncState(AsyncTargetState[] asyncTargetsState) {
        _asyncTargetsState = asyncTargetsState;
    }

    public long getMinimumUnconfirmedKey() {
        if (_asyncTargetsState.length == 0)
            return Long.MAX_VALUE;
        long minConfirmed = Long.MAX_VALUE;
        for (AsyncTargetState asyncTargetState : _asyncTargetsState) {
            if (!asyncTargetState.hadAnyHandshake())
                return 0;
            minConfirmed = Math.min(minConfirmed, asyncTargetState.getGlobalLastConfirmedKey());
        }
        return minConfirmed + 1;
    }

    public long[] getMinimumUnconfirmedBucketKeys() {
        long[] result = null;
        boolean firstIteration = true;
        for (AsyncTargetState asyncTargetState : _asyncTargetsState) {
            long[] bucketLastConfirmedKeys = asyncTargetState.getBucketLastConfirmedKeys();
            if (result == null)
                result = new long[bucketLastConfirmedKeys.length];

            for (int i = 0; i < bucketLastConfirmedKeys.length; i++) {
                long bucketKey = bucketLastConfirmedKeys[i];
                result[i] = firstIteration ? (bucketKey + 1) : Math.min(result[i], (bucketKey + 1));
            }
            firstIteration = false;
        }
        return result;
    }

    public AsyncTargetState[] getAsyncTargetsState() {
        return _asyncTargetsState;
    }

    @Override
    public IReliableAsyncTargetState[] getReliableAsyncTargetsState() {
        return getAsyncTargetsState();
    }

    @Override
    public String toString() {
        return Arrays.toString(_asyncTargetsState);
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        IOUtils.writeObject(out, _asyncTargetsState);
    }

    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        _asyncTargetsState = IOUtils.readObject(in);
    }

    public static class AsyncTargetState implements SmartExternalizable, IReliableAsyncTargetState {
        private static final long serialVersionUID = 1L;

        private String _targetMemberName;
        private long _globalLastConfirmedKey;
        private long[] _bucketLastConfirmedKeys;
        private boolean _hadAnyHandshake;

        public AsyncTargetState() {
        }

        public AsyncTargetState(String targetMemberName, boolean hadAnyHandshake, long globalLastConfirmedKey, long[] bucketLastConfirmedKeys) {
            _targetMemberName = targetMemberName;
            _hadAnyHandshake = hadAnyHandshake;
            _globalLastConfirmedKey = globalLastConfirmedKey;
            _bucketLastConfirmedKeys = new long[bucketLastConfirmedKeys.length];
            for (int i = 0; i < _bucketLastConfirmedKeys.length; ++i)
                _bucketLastConfirmedKeys[i] = bucketLastConfirmedKeys[i];
        }

        public long getGlobalLastConfirmedKey() {
            return _globalLastConfirmedKey;
        }

        public long[] getBucketLastConfirmedKeys() {
            return _bucketLastConfirmedKeys;
        }

        public boolean hadAnyHandshake() {
            return _hadAnyHandshake;
        }

        public void writeExternal(ObjectOutput out) throws IOException {
            IOUtils.writeRepetitiveString(out, _targetMemberName);
            out.writeBoolean(_hadAnyHandshake);
            IOUtils.writeObject(out, _bucketLastConfirmedKeys);
            out.writeLong(_globalLastConfirmedKey);
        }

        public void readExternal(ObjectInput in) throws IOException,
                ClassNotFoundException {
            _targetMemberName = IOUtils.readRepetitiveString(in);
            _hadAnyHandshake = in.readBoolean();
            _bucketLastConfirmedKeys = IOUtils.readObject(in);
            _globalLastConfirmedKey = in.readLong();
        }

        @Override
        public String getTargetMemberName() {
            return _targetMemberName;
        }

        @Override
        public String toString() {
            return "AsyncTargetState [targetMemberName=" + _targetMemberName
                    + ", globalLastConfirmedKey=" + _globalLastConfirmedKey + ", bucketLastConfirmedKeys=" + AbstractMultiBucketSingleFileGroupBacklog.printBucketsKeys(_bucketLastConfirmedKeys) + ", hadAnyHandshake=" + _hadAnyHandshake + "]";
        }

    }


}
