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
 * Represents a remote invocation. Holds invocation values such as the {@link #getLookupName()} and
 * {@link #getMethodName()}.
 *
 * @author kimchy
 */
public interface SpaceRemotingInvocation {

    /**
     * The lookup name of the service. Usually the actual interface class name that is proxied on
     * the client side and implemented on the server side.
     */
    String getLookupName();

    /**
     * The method name of the service that will be executed.
     */
    String getMethodName();

    /**
     * The arguments for the service method execution.
     */
    Object[] getArguments();

    /**
     * Meta arguments that can be passed as part of the invocation. This arguments are not used to
     * invoke the method, but can be used to control ceratin pluggable invocation "aspects".
     */
    Object[] getMetaArguments();

    /**
     * Routing field controls the partition the invocation will be directed to when working with a
     * partitioned space.
     */
    Object getRouting();
}
