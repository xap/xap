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
import com.gigaspaces.security.AuthenticationException;
import com.gigaspaces.security.SecurityException;
import com.gigaspaces.security.directory.CredentialsProvider;
import com.gigaspaces.security.service.SecurityContext;
import com.j_spaces.core.SpaceContext;

import net.jini.id.Uuid;

import java.rmi.RemoteException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Secured proxy manager implementation for a secured space using the new security model.
 *
 * @author Moran Avigdor
 * @since 7.0
 */
@com.gigaspaces.api.InternalApi
public class SpaceProxySecurityManager implements IProxySecurityManager {

    private final IDirectSpaceProxy proxy;
    private volatile CredentialsProvider credentialsProvider;

    /**
     * A cache of remote space Uuids and their authenticated space contexts
     */
    private final ConcurrentHashMap<Uuid, SpaceContext> cache = new ConcurrentHashMap<Uuid, SpaceContext>();

    /**
     * A cache of remote space Uuids and their attached thread context of server-side threads. When
     * an executor task is run at the server-side, it may want to operate on other members in the
     * Cluster or other Spaces/Clusters. In such a case, a new proxy is obtained from within the
     * task. Operations, call {@link #getThreadSpaceContext()}, and only ThreadLocal of the current
     * proxy will be looked-up to avoid overrides from different proxies in the same VM.
     */
    private static final ThreadLocal<Map<Uuid, SpaceContext>> threadContextCache = new ThreadLocal<Map<Uuid, SpaceContext>>();

    /**
     * A flag indicating that the threadContext should be considered
     */
    private static final AtomicInteger threadContextInUse = new AtomicInteger(); //to improve performance of thread-context lookup

    public SpaceProxySecurityManager(IDirectSpaceProxy proxy) {
        this.proxy = proxy;
    }

    @Override
    public CredentialsProvider getCredentialsProvider() {
        return credentialsProvider;
    }

    @Override
    public void initialize(CredentialsProvider credentialsProvider) {
        this.credentialsProvider = credentialsProvider;
    }

    @Override
    public SecurityContext login(CredentialsProvider credentialsProvider) throws RemoteException {

        if (credentialsProvider == null) {
            throw new SecurityException("Invalid credentials provider");
        }

        this.credentialsProvider = credentialsProvider;
        // Workaround to solve failover bug GS-7546
        // If primary fails, and a login operation is performed on stale
        // proxy, a RemoteException is thrown.
        // try and login to one of the known remote proxies
        // if not successful, throw original exception
        // TODO [@author moran/anna] login/logout should be a broadcast operation
        IRemoteSpace rj = proxy.getProxyRouter().getAnyAvailableSpace();
        if (rj == null) {
            rj = proxy.getRemoteJSpace();
        }
        SecurityContext securityContext = rj.login(new SecurityContext(credentialsProvider));

        cache.clear();
        cacheIt(rj, createSpaceContext(securityContext));
        return securityContext;
    }

    @Override
    public SpaceContext acquireContext(IRemoteSpace rj) throws RemoteException {

        SpaceContext threadSpaceContext = getThreadSpaceContext();
        if (threadSpaceContext != null) {
            return threadSpaceContext;
        }

        SpaceContext cachedSpaceContext = cache.get(rj.getSpaceUuid());
        if (cachedSpaceContext != null) {
            return cachedSpaceContext;
        }

        final SpaceContext spaceContext = acquireContext(rj, this.credentialsProvider);
        cachedSpaceContext = cacheIt(rj, spaceContext);

        return cachedSpaceContext;
    }

    /**
     * Acquire context with the provided credentials without caching the result at the proxy.
     * The usage resembles a session which needs to be passed explicitly on each remote API call.
     *
     * @param rs Remote Space proxy
     * @param cp Credentials Provider
     * @return a Space Context holding a Security Context propagated after login with the credentials
     * @throws RemoteException
     */
    @Override
    public SpaceContext acquireContext(IRemoteSpace rs, CredentialsProvider cp) throws RemoteException {

        if (cp == null) {
            throw new AuthenticationException("No credentials were provided");
        }

        final SecurityContext securityContext = rs.login(new SecurityContext(cp));
        final SpaceContext spaceContext = createSpaceContext(securityContext);
        return spaceContext;
    }

    /**
     * Cache the security context as a SpaceContext mapped to this remote space.
     *
     * @param rj              remote space proxy
     * @param spaceContext    space context with authenticated security context
     * @return space context
     */
    private SpaceContext cacheIt(IRemoteSpace rj, SpaceContext spaceContext) throws RemoteException {
        cache.put(rj.getSpaceUuid(), spaceContext);
        return spaceContext;
    }

    /**
     * Create a SpaceContext with authenticated security context
     * @param securityContext authenticated security context
     * @return a SpaceContext
     */
    private SpaceContext createSpaceContext(SecurityContext securityContext) {
        final SecurityContext lightSecurityContext = new SecurityContext(securityContext);
        return proxy.getProxyRouter().getDefaultSpaceContext().createCopy(lightSecurityContext);
    }


    @Override
    public SpaceContext getThreadSpaceContext() {
        SpaceContext result = null;
        if (threadContextInUse.get() > 0) {
            Map<Uuid, SpaceContext> map = threadContextCache.get();
            result = map == null ? null : map.get(proxy.getReferentUuid());
        }
        return result;
    }

    @Override
    public SpaceContext setThreadSpaceContext(SpaceContext sc) {
        Map<Uuid, SpaceContext> map = threadContextCache.get();
        SpaceContext prev;

        if (sc != null) {
            if (map == null) {
                map = new HashMap<Uuid, SpaceContext>();
                threadContextCache.set(map);
                threadContextInUse.incrementAndGet();
            }
            prev = map.put(proxy.getReferentUuid(), sc);
        } else {
            if (map != null) {
                prev = map.remove(proxy.getReferentUuid());
                if (map.isEmpty()) {
                    threadContextCache.remove();
                    threadContextInUse.decrementAndGet();
                }
            } else
                prev = null;
        }
        return prev;
    }
}
