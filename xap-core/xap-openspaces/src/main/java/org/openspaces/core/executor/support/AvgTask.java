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

import com.gigaspaces.serialization.SmartExternalizable;
import org.openspaces.core.executor.Task;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;

/**
 * A sum distrubted task that accepts a {@link org.openspaces.core.executor.Task} to delegate the
 * actual execution to and implements the {@link #reduce(java.util.List)} operation.
 *
 * <p>By defualt, throws an exception if one of the execution fails. {@link #ignoreExceptions()} can
 * be called to only perform the operation on all the successful operations, ignoring the failed
 * ones.
 *
 * <p>Can accept an optioanl {@link com.gigaspaces.async.AsyncResultFilter}.
 *
 * @author kimchy
 * @see SumReducer
 */
public class AvgTask<T extends Number, R extends Number> extends AbstractDelegatingDistributedTask<T, R> implements SmartExternalizable {

    private static final long serialVersionUID = -6970700450888522663L;

    private transient AvgReducer<T, R> reducer;

    /**
     * Here for externalizable.
     */
    public AvgTask() {
        super();
    }

    /**
     * Constructs a new sum distributed task that delegates the actual execution to th provided
     * task.
     *
     * @param task The task to delegate the execution to.
     */
    public AvgTask(Class<R> reduceType, Task<T> task) throws IllegalArgumentException {
        super(task);
        this.reducer = new AvgReducer<T, R>(reduceType);
    }

    /**
     * Constructs a new sum distributed task that delegates the actual execution to th provided
     * task.
     *
     * @param task   The task to delegate the execution to.
     * @param filter A result filter to be called for each result
     */
    public AvgTask(Class<R> reduceType, Task<T> task, AsyncResultFilter<T> filter) throws IllegalArgumentException {
        super(task, filter);
        this.reducer = new AvgReducer<T, R>(reduceType);
    }

    /**
     * Sests the {@link #reduce(java.util.List)} to ignore failed invocations.
     */
    public AvgTask ignoreExceptions() {
        this.reducer.ignoreExceptions();
        return this;
    }

    /**
     * Performs the actual sum operation by delegating to its internal {@link SumReducer}.
     */
    public R reduce(List<AsyncResult<T>> results) throws Exception {
        return reducer.reduce(results);
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        super._writeExternal(out);
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super._readExternal(in);
    }
}