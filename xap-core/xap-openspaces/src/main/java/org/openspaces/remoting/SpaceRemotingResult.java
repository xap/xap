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


package org.openspaces.remoting;

/**
 * A remote invocation result holding either an exception (that occurred during the execution of
 * remote service) or the actual result data.
 *
 * <p>In order to correctly handle results, first check {@link #getException()} for possible
 * exception during the remote invocation. If a <code>null</code> value is returned, check the
 * actual result using {@link #getResult()}.
 *
 * @author kimchy
 */
public interface SpaceRemotingResult<T> {

    /**
     * Returns the routing index for the given result in cases where the remote invocation was
     * directed to a specific cluster instance.
     */
    Object getRouting();

    /**
     * The result of the remote invocation. Note, <code>null</code> value might mean that remote
     * service returned null/void, but it also might means that there might be an {@link
     * #getException()}.
     */
    T getResult();

    /**
     * An exception that occured during the remote invocation. <code>null</code> value means there
     * was no exception.
     */
    Throwable getException();

    /**
     * The cluster instance id this result was executed on.
     */
    Integer getInstanceId();
}