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
import com.gigaspaces.internal.query.explainplan.SingleExplainPlan;
import com.gigaspaces.internal.remoting.RemoteOperationResult;
import com.gigaspaces.internal.utils.Textualizable;
import com.gigaspaces.internal.utils.Textualizer;
import com.gigaspaces.serialization.SmartExternalizable;
import com.j_spaces.core.exception.internal.ProxyInternalSpaceException;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * @author Niv Ingberg
 * @since 9.0.0
 */
public abstract class SpaceOperationResult implements RemoteOperationResult, SmartExternalizable, Textualizable {
    private static final long serialVersionUID = 1L;

    private Exception _executionException;
    private SingleExplainPlan _SingleExplainPlan;

    @Override
    public Exception getExecutionException() {
        return _executionException;
    }

    @Override
    public void setExecutionException(Exception exception) {
        this._executionException = exception;
    }

    public boolean hasException() {
        return _executionException != null;
    }

    public SingleExplainPlan getExplainPlan() {
        return _SingleExplainPlan;
    }

    public void setExplainPlan(SingleExplainPlan plan) {
        this._SingleExplainPlan = plan;
    }

    protected static void onUnexpectedException(Exception e) {
        if (e instanceof RuntimeException)
            throw (RuntimeException) e;

        throw new ProxyInternalSpaceException("Unexpected exception: " + e.getMessage(), e);
    }

    @Override
    public String toString() {
        return Textualizer.toString(this);
    }

    @Override
    public void toText(Textualizer textualizer) {
        textualizer.appendIfNotNull("executionException", _executionException);
    }

    private static final short FLAG_EXECUTION_EXCEPTION = 1 << 0;
    private static final short FLAG_EXPLAIN_PLAN = 1 << 1;

    @Override
    public void writeExternal(ObjectOutput out)
            throws IOException {
        final short flags = buildFlags();
        out.writeShort(flags);
        if (flags != 0) {
            if (_executionException != null)
                IOUtils.writeObject(out, _executionException);
            if (_SingleExplainPlan != null)
                IOUtils.writeObject(out, _SingleExplainPlan);
        }
    }

    @Override
    public void readExternal(ObjectInput in)
            throws IOException, ClassNotFoundException {
        final short flags = in.readShort();
        if (flags != 0) {
            if ((flags & FLAG_EXECUTION_EXCEPTION) != 0)
                this._executionException = IOUtils.readObject(in);
            if ((flags & FLAG_EXPLAIN_PLAN) != 0)
                this._SingleExplainPlan = IOUtils.readObject(in);
        }
    }

    private short buildFlags() {
        short flags = 0;

        if (_executionException != null)
            flags |= FLAG_EXECUTION_EXCEPTION;

        if (_SingleExplainPlan != null)
            flags |= FLAG_EXPLAIN_PLAN;

        return flags;
    }
}
