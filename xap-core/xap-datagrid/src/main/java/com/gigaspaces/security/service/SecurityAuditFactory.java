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

package com.gigaspaces.security.service;

import com.gigaspaces.logger.LogLevel;
import com.gigaspaces.security.SecurityException;
import com.gigaspaces.security.audit.SecurityAudit;

import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A factory for creating {@link SecurityAudit} implementation
 *
 * Created by moran on 4/11/16.
 */
@com.gigaspaces.api.InternalApi
public class SecurityAuditFactory {

    private static final Logger logger = Logger.getLogger(SecurityAuditFactory.class.getName());

    /**
     * The property key identifying the security audit class in a properties file/object
     */
    public static final String SECURITY_AUDIT_CLASS_PROPERTY_KEY = "com.gs.security.security-audit.class";

    /**
     * Reference-implementation for backwards compatibility
     */
    private static final String DEFAULT_SECURITY_AUDIT_CLASS_REFERENCE_IMPL = "com.gigaspaces.security.audit.LoggerSecurityAudit";

    /**
     * Creates a {@link SecurityAudit} instance using the provided security properties. The property
     * key {@link #SECURITY_AUDIT_CLASS_PROPERTY_KEY} identifies the class name to use to load the
     * security audit implementation.
     *
     * @param securityProperties The security properties to use to create the security audit and
     *                           underlying components.
     * @return The security audit instance.
     * @throws SecurityException if failed to create the security audit.
     */
    public static SecurityAudit createSecurityAudit(Properties securityProperties) throws SecurityException {
        if (logger.isLoggable(Level.FINE)) {
            String property = securityProperties.getProperty(SECURITY_AUDIT_CLASS_PROPERTY_KEY);
            if (property != null) {
                logger.fine("Security security-audit class: " + property);
            }
        }

        final String securityAuditClassName = securityProperties.getProperty(SECURITY_AUDIT_CLASS_PROPERTY_KEY, DEFAULT_SECURITY_AUDIT_CLASS_REFERENCE_IMPL);
        final boolean isDefault = securityAuditClassName.equals(DEFAULT_SECURITY_AUDIT_CLASS_REFERENCE_IMPL);
        try {

            Class<? extends SecurityAudit> securityAuditClass = Thread.currentThread().getContextClassLoader()
                    .loadClass(securityAuditClassName).asSubclass(SecurityAudit.class);

            return securityAuditClass.newInstance();

        } catch (Exception e) {
            (isDefault ? LogLevel.DEBUG : LogLevel.SEVERE).log(logger, "Failed to create an instance of SecurityAudit class", e);
            if (!isDefault) {
                throw new SecurityException("Failed to create Security Audit", e);
            }
            return null; //default implementation not found, don't audit
        }
    }
}
