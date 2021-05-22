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


package org.openspaces.core.executor.juc;

import com.gigaspaces.serialization.SmartExternalizable;
import org.openspaces.core.executor.Task;
import org.openspaces.core.executor.TaskRoutingProvider;
import org.openspaces.core.executor.support.ProcessObjectsProvider;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.concurrent.Callable;

/**
 * An adapter allowing to execute a {@link Callable} using Space task executors.
 *
 * @author kimchy
 */
public class CallableTaskAdapter<T extends Serializable> implements Task<T>, ProcessObjectsProvider, TaskRoutingProvider, SmartExternalizable {

    private static final long serialVersionUID = -4269560297517277737L;

    private Callable<T> callable;

    /**
     * Here just for externalizable.
     */
    public CallableTaskAdapter() {
    }

    /**
     * Constructs a new callable task adapter with the callable to <code>call</code>.
     */
    public CallableTaskAdapter(Callable<T> callable) {
        this.callable = callable;
    }

    /**
     * Simply delegates the execution to {@link java.util.concurrent.Callable#call()}.
     */
    public T execute() throws Exception {
        return callable.call();
    }

    /**
     * Returns the callable passed so it will be processed on the node it is executed on as well.
     */
    public Object[] getObjectsToProcess() {
        return new Object[]{callable};
    }

    /**
     * Tries to extract the routing information form the task.
     */
    public Object getRouting() {
        return callable;
    }

    /**
     * Returns the embedded callable.
     */
    protected Callable<T> getCallable() {
        return this.callable;
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(callable);
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        callable = (Callable<T>) in.readObject();
    }
}
