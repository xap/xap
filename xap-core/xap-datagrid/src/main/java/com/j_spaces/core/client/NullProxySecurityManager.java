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

import com.gigaspaces.internal.client.spaceproxy.IDirectSpaceProxy;
import com.gigaspaces.internal.server.space.IRemoteSpace;
import com.gigaspaces.security.directory.CredentialsProvider;
import com.gigaspaces.security.service.SecurityContext;
import com.j_spaces.core.SpaceContext;

import java.rmi.RemoteException;

/**
 * Used for non-secured space
 *
 * @author Moran Avigdor
 * @since 7.0
 */
@com.gigaspaces.api.InternalApi
public class NullProxySecurityManager implements IProxySecurityManager {
    private final IDirectSpaceProxy _spaceProxy;

    public NullProxySecurityManager(IDirectSpaceProxy spaceProxy) {
        _spaceProxy = spaceProxy;
    }

    @Override
    public void initialize(CredentialsProvider credentialsProvider) {

    }

    @Override
    public SpaceContext acquireContext(IRemoteSpace rj)
            throws RemoteException {
        return _spaceProxy.getProxyRouter().getDefaultSpaceContext();
    }

    @Override
    public SpaceContext acquireContext(IRemoteSpace remoteSpace, CredentialsProvider credentialsProvider) throws RemoteException {
        return _spaceProxy.getProxyRouter().getDefaultSpaceContext();
    }

    @Override
    public CredentialsProvider getCredentialsProvider() {
        return null;
    }

    @Override
    public SecurityContext login(CredentialsProvider credentialsProvider) {
        return null;
    }

    @Override
    public SpaceContext getThreadSpaceContext() {
        return null;
    }

    @Override
    public SpaceContext setThreadSpaceContext(SpaceContext sc) {
        return null;
    }
}
