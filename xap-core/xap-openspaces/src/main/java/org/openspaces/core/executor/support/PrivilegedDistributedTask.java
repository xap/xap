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


package org.openspaces.core.executor.support;

import com.gigaspaces.async.AsyncResult;
import com.gigaspaces.async.AsyncResultFilter;
import com.gigaspaces.async.AsyncResultFilterEvent;

import com.gigaspaces.serialization.SmartExternalizable;
import org.openspaces.core.executor.DistributedTask;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.List;

/**
 * A delegating distrubuted task (with an optional filter) that runs under established access
 * control settings.
 *
 * @author kimchy
 */
public class PrivilegedDistributedTask<T extends Serializable, R> extends AbstractDelegatingDistributedTask<T, R> implements SmartExternalizable {

    private static final long serialVersionUID = 8798827598285224843L;

    private transient T result;

    private transient R reduceResult;

    private transient Exception exception;

    /**
     * Here for Externalizable.
     */
    public PrivilegedDistributedTask() {
        super();
    }

    /**
     * Constructs a new privileged task wrapping the actual task to execute.
     */
    public PrivilegedDistributedTask(DistributedTask<T, R> task) {
        super(task);
    }

    /**
     * Constructs a new privileged task wrapping the actual task and filter to execute.
     */
    public PrivilegedDistributedTask(DistributedTask<T, R> task, AsyncResultFilter<T> filter) {
        super(task, filter);
    }

    /**
     * Exeutes the provided task under access controls.
     */
    public T execute() throws Exception {
        AccessControlContext acc = AccessController.getContext();
        AccessController.doPrivileged(new PrivilegedAction<Object>() {
            public Object run() {
                try {
                    result = getDelegatedTask().execute();
                } catch (Exception ex) {
                    exception = ex;
                }
                return null;
            }
        }, acc);
        if (exception != null)
            throw exception;
        else
            return result;
    }

    /**
     * Reduces the provided task under access controls.
     */
    public R reduce(final List<AsyncResult<T>> results) throws Exception {
        AccessControlContext acc = AccessController.getContext();
        AccessController.doPrivileged(new PrivilegedAction<Object>() {
            public Object run() {
                try {
                    reduceResult = ((DistributedTask<T, R>) getDelegatedTask()).reduce(results);
                } catch (Exception ex) {
                    exception = ex;
                }
                return null;
            }
        }, acc);
        if (exception != null)
            throw exception;
        else
            return reduceResult;
    }

    /**
     * Executes the filter (if provided) under access controls.
     */
    public Decision onResult(final AsyncResultFilterEvent<T> event) {
        if (getFilter() == null) {
            return Decision.CONTINUE;
        }
        AccessControlContext acc = AccessController.getContext();
        return AccessController.doPrivileged(new PrivilegedAction<Decision>() {
            public Decision run() {
                return getFilter().onResult(event);
            }
        }, acc);
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        super._writeExternal(out);
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super._readExternal(in);
    }
}
