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

//Internal Doc
package com.gigaspaces.security.service;

import com.gigaspaces.internal.client.spaceproxy.ISpaceProxy;
import com.gigaspaces.internal.io.MarshObject;
import com.gigaspaces.internal.io.MarshObjectConvertor;
import com.gigaspaces.security.AccessDeniedException;
import com.gigaspaces.security.Authentication;
import com.gigaspaces.security.AuthenticationException;
import com.gigaspaces.security.AuthenticationToken;
import com.gigaspaces.security.SecurityException;
import com.gigaspaces.security.SecurityFactory;
import com.gigaspaces.security.SecurityManager;
import com.gigaspaces.security.audit.SecurityAudit;
import com.gigaspaces.security.authorities.GrantedAuthorities;
import com.gigaspaces.security.authorities.Privilege;
import com.gigaspaces.security.directory.CredentialsProvider;
import com.gigaspaces.security.directory.DefaultCredentialsProvider;
import com.gigaspaces.security.directory.User;
import com.gigaspaces.security.directory.UserDetails;
import com.gigaspaces.security.session.SessionDetails;
import com.gigaspaces.security.session.SessionId;
import com.j_spaces.core.IJSpace;
import com.j_spaces.core.SpaceContext;
import com.j_spaces.core.SpaceContextHelper;
import com.j_spaces.kernel.SystemProperties;

import java.io.IOException;
import java.io.InputStream;
import java.rmi.RemoteException;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Server-side security interceptor, responsible for authentication against the underlying {@link
 * SecurityManager}, handing of authentication tokens to authenticated users, and intercepting any
 * access to resources.
 *
 * @author Moran Avigdor
 * @since 7.0.1
 */
@com.gigaspaces.api.InternalApi
public class SecurityInterceptor {
    private static final Logger logger = LoggerFactory.getLogger("com.gigaspaces.security");

    private final SecurityManager securityManager;
    private final SecurityAudit securityAudit; //may be null if SecurityAuditFactory can't load the default implementation
    private final ConcurrentHashMap<MarshObject, AuthenticationToken> marshedCache = new ConcurrentHashMap<MarshObject, AuthenticationToken>();
    private final ConcurrentHashMap<AuthenticationToken, SessionDetails> cache = new ConcurrentHashMap<AuthenticationToken, SessionDetails>();
    private final SecurityTrustInterceptor trustInterceptor = new SecurityTrustInterceptor();

    /*
     * Trusted user gains a trusted token.
     */
    private final AuthenticationToken trustedToken = new AuthenticationToken(SessionId.generateIdentifier());
    private final UserDetails trustedUser = new TrustedUser(String.valueOf(UUID.randomUUID()));
    private final CredentialsProvider trustedCredentialsProvider = new DefaultCredentialsProvider(trustedUser);

    private static final class TrustedUser extends User {
        private static final long serialVersionUID = 1L;

        public TrustedUser(String uid) {
            super(uid, uid); //dummy
        }

        @Override
        public int hashCode() {
            return -1;
        }

        @Override //equals across partitions or in same VM
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            return (this.getClass() == obj.getClass());
        }
    }

    public SecurityInterceptor(String component) {
        this(component, new Properties(), false);
    }

    /**
     * Construct an interceptor for this component, applying any additional properties provided. A
     * {@link SecurityManager} can be injected through this properties object.
     *
     * @param component     component name (can be null)
     * @param props         custom properties provided.
     * @param useMinusDLast <code>true</code> if should first find security properties file matching
     *                      the component name, and if not found, resolve by matching the sys-prop
     *                      provided name. Usually when running inside a container.
     *                      <code>false</code> if should first resolve by matching the sys-prop
     *                      provided name.
     */
    public SecurityInterceptor(String component, Properties props, boolean useMinusDLast) {
        if (logger.isInfoEnabled())
            logger.info("Security enabled for " + component);
        SecurityManager injectedSecurityManager = null;
        //verify property doesn't exist as a String before trying to cast into SecurityManager
        if (props.get(SecurityManager.SECURITY_MANAGER_CLASS_PROPERTY_KEY) instanceof SecurityManager) {
            injectedSecurityManager = (SecurityManager) props.remove(SecurityManager.SECURITY_MANAGER_CLASS_PROPERTY_KEY);
        }
        if (injectedSecurityManager != null) {
            securityAudit = SecurityAuditFactory.createSecurityAudit(props);
            securityManager = injectedSecurityManager;
        } else {
            Properties securityProperties = SecurityFactory.loadComponentSecurityProperties(component, useMinusDLast);
            // merge the properties (props override the ones we loaded)
            securityProperties.putAll(props);

            securityAudit = SecurityAuditFactory.createSecurityAudit(securityProperties);
            securityManager = SecurityFactory.createSecurityManager(securityProperties);
        }
    }

    /**
     * Authenticates the users' details (username and password) specified as part of the
     * security-context against the underlying {@link SecurityManager}. An authenticated user is
     * given a token for subsequent usage. Audit successful/failed authentication requests.
     *
     * @param securityContext The security context holding the user details.
     * @return an authenticated security context holding the authentication token.
     */
    public SecurityContext authenticate(SecurityContext securityContext) {
        if (securityContext == null) {
            throw new SecurityException("Invalid security context");
        }
        UserDetails userDetails = securityContext.getUserDetails();
        if (userDetails == null) {
            throw new AuthenticationException("No authentication details were supplied");
        }

        if (isTrusted(userDetails)) {
            return createTrustedContext();
        }

        try {
            Authentication authentication = securityManager.authenticate(userDetails);
            MarshObjectConvertor marshObjectConvertor = new MarshObjectConvertor(); //not thread safe
            MarshObject marshObject = new MarshObject(marshObjectConvertor.toBinary(userDetails));
            AuthenticationToken authenticationToken = new AuthenticationToken(SessionId.generateIdentifier());
            AuthenticationToken cachedAuthenticationToken = marshedCache.putIfAbsent(marshObject, authenticationToken);
            if (cachedAuthenticationToken != null) {
                authenticationToken = cachedAuthenticationToken;
            }

            SessionDetails sessionDetails = new SessionDetails(authentication, securityContext);
            cache.put(authenticationToken, sessionDetails);

            SecurityContext context = new SecurityContext(authentication.getUserDetails(), authenticationToken);
            if (securityAudit != null) {
                securityAudit.authenticationSuccessful(securityContext, context);
            }
            return context;

        } catch (AuthenticationException authenticationException) {
            if (securityAudit != null) {
                securityAudit.authenticationFailed(securityContext, authenticationException);
            }
            throw authenticationException;
        } catch (IOException e) {
            throw new AuthenticationException("Could not marshal user details", e);
        }
    }

    /**
     * Internal trust mechanism used by embedded workers, query processors, etc.
     *
     * @return a security context initialized with a trusted authenticated token.
     */
    private SecurityContext createTrustedContext() {
        return new SecurityContext(null, trustedToken);
    }

    public boolean isTrusted(UserDetails userDetails) {
        return trustedUser.equals(userDetails);
    }

    /**
     * Internal (VM) trust mechanism; acquires a trusted token and sets the internal proxy.
     */
    public void trustProxy(IJSpace proxy) throws RemoteException {
        ((ISpaceProxy) proxy).login(trustedCredentialsProvider);
    }

    /**
     * Intercepts any requests against the provided authentication token and the privilege needed to
     * access the resource.
     *
     * @param privilege The privilege required to access the resource.
     * @param className The class name to intercept if an operation filter was provided.
     * @throws AuthenticationException if the authentication token provided is invalid.
     * @throws AccessDeniedException   if the authenticated user lacks privileges required by the
     *                                 accessed resource.
     */
    public void intercept(SecurityContext securityContext, Privilege privilege, String className) {
        if (securityContext == null) {
            throw new SecurityException("Invalid security context");
        }
        AuthenticationToken token = securityContext.getAuthenticationToken();
        if (token == null) {
            throw new AuthenticationException("Authentication token is invalid");
        }
        if (trustInterceptor.verifyTrust(securityContext)) {
            return;
        }
        SessionDetails sessionDetails = cache.get(token);
        if (sessionDetails == null) {
            if (token.equals(trustedToken)) {
                return;
            }
            if (securityAudit != null) {
                securityAudit.authenticationInvalid(token);
            }
            throw new AuthenticationException("Authentication session is invalid");
        }

        Authentication authentication = sessionDetails.getAuthentication();
        GrantedAuthorities grantedAuthorities = authentication.getGrantedAuthorities();
        boolean granted = grantedAuthorities.isGranted(privilege, className);
        if (!granted) {
            AccessDeniedException accessDeniedException = new AccessDeniedException("User ["
                    + authentication.getUserDetails().getUsername() + "] lacks [" + privilege
                    + "] privileges" + (className == null ? "" : " for class [" + className + "]"));
            if (securityAudit != null) {
                securityAudit.accessDenied(securityContext, sessionDetails, privilege, className);
            }
            throw accessDeniedException;
        }
        if (securityAudit != null) {
            securityAudit.accessGranted(securityContext, sessionDetails, privilege, className);
        }
        SecurityContextAccessor.fillSecurityContext(securityContext, sessionDetails);
    }

    /**
     * Retrieve user details by the authentication token to allow login into first-time cluster
     * member proxies.
     *
     * @param authenticationToken a token
     * @return the user details corresponding to this token.
     * @throws AuthenticationException if the authentication token is invalid.
     */
    public UserDetails getUserDetails(AuthenticationToken authenticationToken) {
        SessionDetails sessionDetails = cache.get(authenticationToken);
        if (sessionDetails == null) {
            throw new AuthenticationException("Authentication token is invalid");
        }
        return sessionDetails.getAuthentication().getUserDetails();
    }

    /**
     * Extract the original security context and create a trusted security context wrapper.
     *
     * @param spaceContext The space context
     * @return a space context holding a trusted security context wrapping the original security
     * context.
     */
    public SpaceContext trustContext(SpaceContext spaceContext) {
        SecurityContext trustedContext = trustInterceptor.trust(SpaceContextHelper.getSecurityContext(spaceContext));
        return spaceContext.createCopy(trustedContext);
    }

    /**
     * If security context should not be passed to the filter or audit.
     *
     * @return true if should bypass the filter.
     */
    public boolean shouldBypassFilter(SecurityContext securityContext) {
        return (isTrusted(securityContext.getUserDetails()) || trustInterceptor.verifyTrust(securityContext));
    }
}
