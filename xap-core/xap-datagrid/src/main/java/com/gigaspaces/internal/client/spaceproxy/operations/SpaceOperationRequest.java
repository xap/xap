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

package com.gigaspaces.internal.client.spaceproxy.operations;

import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.remoting.RemoteOperationRequest;
import com.gigaspaces.internal.utils.Textualizable;
import com.gigaspaces.internal.utils.Textualizer;
import com.gigaspaces.serialization.SmartExternalizable;
import com.j_spaces.core.SpaceContext;

import net.jini.core.transaction.Transaction;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;

/**
 * @author Niv Ingberg
 * @since 9.0.0
 */
public abstract class SpaceOperationRequest<TResult extends SpaceOperationResult> implements RemoteOperationRequest<TResult>, Cloneable, SmartExternalizable, Textualizable {
    private static final long serialVersionUID = 1L;

    // TODO: consider merging SpaceContext with Request
    private SpaceContext _spaceContext;
    private transient TResult _remoteOperationResult;

    @Override
    public String toString() {
        return Textualizer.toString(this);
    }

    @Override
    public void toText(Textualizer textualizer) {
    }

    @Override
    public TResult getRemoteOperationResult() {
        return _remoteOperationResult;
    }

    @Override
    public void setRemoteOperationResult(TResult remoteOperationResult) {
        _remoteOperationResult = remoteOperationResult;
    }

    @Override
    public void setRemoteOperationExecutionError(Exception error) {
        _remoteOperationResult = createRemoteOperationResult();
        _remoteOperationResult.setExecutionException(error);
    }

    @Override
    public boolean processPartitionResult(TResult remoteOperationResult, List<TResult> previousResults, int numOfPartitions) {
        throw new UnsupportedOperationException("This method must be overridden if execution type is broadcast");
    }

    @Override
    public Object getAsyncFinalResult() throws Exception {
        return getRemoteOperationResult();
    }

    public Transaction getTransaction() {
        return null;
    }

    @Override
    public boolean isBlockingOperation() {
        return false;
    }

    @Override
    public boolean isDedicatedPoolRequired() {
        return false;
    }

    @Override
    public boolean isDirectExecutionEnabled() {
        return false;
    }

    @Override
    public boolean processUnknownTypeException(List<Integer> positions) {
        return false;
    }

    public void setSpaceContext(SpaceContext spaceContext) {
        _spaceContext = spaceContext;
    }

    @Override
    public SpaceContext getSpaceContext() {
        return _spaceContext;
    }

    @Override
    @SuppressWarnings("unchecked")
    public RemoteOperationRequest<TResult> createCopy(int targetPartitionId) {
        return (RemoteOperationRequest<TResult>) this.clone();
    }

    @Override
    protected Object clone() {
        try {
            return super.clone();
        } catch (CloneNotSupportedException e) {
            throw new IllegalStateException("Failed to clone a cloneable object", e);
        }
    }

    private static final short FLAG_SPACE_CONTEXT = 1 << 0;

    @Override
    public void writeExternal(ObjectOutput out)
            throws IOException {
        final short flags = buildFlags();
        out.writeShort(flags);
        if (flags != 0) {
            if (_spaceContext != null)
                IOUtils.writeObject(out, _spaceContext);
        }
    }

    @Override
    public void readExternal(ObjectInput in)
            throws IOException, ClassNotFoundException {
        final short flags = in.readShort();
        if (flags != 0) {
            if ((flags & FLAG_SPACE_CONTEXT) != 0)
                this._spaceContext = IOUtils.readObject(in);
        }
    }

    private short buildFlags() {
        short flags = 0;

        if (_spaceContext != null)
            flags |= FLAG_SPACE_CONTEXT;

        return flags;
    }

    public boolean beforeOperationExecution(boolean isEmbedded) {
        return true;
    }

    public void afterOperationExecution(int partitionId) {
    }

    /**
     * @return true if the request execution caused entries to be locked under transaction, false
     * otherwise.
     */
    public boolean hasLockedResources() {
        return true;
    }

    @Override
    public boolean requiresPartitionedPreciseDistribution() {
        return false;
    }

    @Override
    public int getPreciseDistributionGroupingCode() {
        throw new UnsupportedOperationException();
    }

    public boolean supportsSecurity() {
        return true;
    }
}
