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

package com.j_spaces.core.client;

import com.gigaspaces.internal.server.space.IRemoteSpace;
import com.gigaspaces.security.directory.CredentialsProvider;
import com.gigaspaces.security.service.SecurityContext;
import com.j_spaces.core.SpaceContext;
import com.j_spaces.kernel.SystemProperties;

import java.rmi.RemoteException;

/**
 * @author anna
 * @since 6.5
 */
public interface IProxySecurityManager {
    public static final boolean SUPPORT_TRANSACTION_AUTHENTICATION = !Boolean.getBoolean(SystemProperties.SECURITY_DISABLE_TRANSACTION_AUTHENTICATION);

    CredentialsProvider getCredentialsProvider();

    void initialize(CredentialsProvider credentialsProvider);

    SecurityContext login(CredentialsProvider credentialsProvider) throws RemoteException;

    SpaceContext acquireContext(IRemoteSpace rj) throws RemoteException;

    SpaceContext acquireContext(IRemoteSpace remoteSpace, CredentialsProvider credentialsProvider) throws RemoteException;

    SpaceContext getThreadSpaceContext();

    SpaceContext setThreadSpaceContext(SpaceContext sc);

}