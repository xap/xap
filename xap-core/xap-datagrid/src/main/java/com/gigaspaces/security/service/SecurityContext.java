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

import com.gigaspaces.security.Authentication;
import com.gigaspaces.security.AuthenticationToken;
import com.gigaspaces.security.audit.AuditDetails;
import com.gigaspaces.security.directory.CredentialsProvider;
import com.gigaspaces.security.directory.UserDetails;
import com.gigaspaces.security.encoding.EncodingException;
import com.gigaspaces.security.encoding.KeyFactory;
import com.gigaspaces.security.encoding.aes.AesContentEncoder;
import com.gigaspaces.security.session.SessionDetails;
import com.gigaspaces.serialization.SmartExternalizable;
import com.j_spaces.core.filters.ISpaceFilter;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import javax.crypto.SecretKey;

/**
 * Security context passed between proxy and server.
 *
 * @author Moran Avigdor
 * @since 7.0.1
 */

public class SecurityContext implements SmartExternalizable {

    private static final long serialVersionUID = 1L;

    private UserDetails userDetails;
    private AuthenticationToken authenticationToken;
    private AuditDetails auditDetails;
    private transient Authentication authentication;

    /*
     * Construct a Secret key to be used for encoding the user details passed on the first
     * authentication request.
     * 
     * Note: AesContentEncoder is not concurrent. A call to doFinal(..) resets the Cipher object to
     * the state it was in when initialized via a call to init(..). Therefore, do not make it static
     * or you will get stream corruption and bad padding exceptions.
     */
    private final static SecretKey secretKey = generateKey();

    /**
     * Attempts to load a custom secret key. If not found, uses default private secret key. The
     * custom secret key must be available to both client and server to be able to encode and decode
     * at each end the user details.
     *
     * @return a secret key.
     */
    private static SecretKey generateKey() {
        SecretKey secretKey = KeyFactory.loadKey("gs-keystore.key");
        if (secretKey == null) {
            secretKey = KeyFactory.generateKey(new byte[]{-69, -26, 70, -58, 49, 81, 104,
                    -9, -105, 93, -114, 26, 75, -60, 96, -39}, "AES");
        }
        return secretKey;
    }

    /**
     * {@link Externalizable} public no-args constructor
     */
    public SecurityContext() {
    }

    public SecurityContext(CredentialsProvider credentialsProvider) {
        this.userDetails = credentialsProvider.getUserDetails();
        this.auditDetails = new AuditDetails();
    }

    /**
     * Security context constructed for subsequent session interaction.
     *
     * @param securityContext a token for this session.
     */
    public SecurityContext(SecurityContext securityContext) {
        this.authenticationToken = securityContext.getAuthenticationToken();
        this.auditDetails = securityContext.getAuditDetails();
    }

    /**
     * Security context constructed upon authentication.
     *
     * @param userDetails         authenticated user details with populated authorities.
     * @param authenticationToken a token for this session.
     */
    public SecurityContext(UserDetails userDetails, AuthenticationToken authenticationToken) {
        this.userDetails = userDetails;
        this.authenticationToken = authenticationToken;
    }

    /**
     * @return the userDetails (may be <code>null</code>); <code>null</code> when context is
     * transfered after a successful authentication; non-<code>null</code> when accessed from within
     * an {@link ISpaceFilter}.
     */
    public UserDetails getUserDetails() {
        return userDetails;
    }

    /* package level access only @see {@link SecurityContextAccessor} */
    void applySessionDetails(SessionDetails sessionDetails) {
        this.userDetails = sessionDetails.getAuthentication().getUserDetails();
        this.authentication = sessionDetails.getAuthentication();
        this.auditDetails = sessionDetails.getAuditDetails();
    }

    /**
     * @return the authentication (may be <code>null</code>); <code>null</code> when context is
     * transfered before a successful authentication; non-<code>null</code> when accessed from
     * within an {@link ISpaceFilter} after successful authentication.
     */
    public Authentication getAuthentication() {
        if (authentication == null) {
            authentication = new Authentication(userDetails);
        }
        return authentication;
    }

    /**
     * @return the authenticationToken (may be <code>null</code>); <code>null</code> when context is
     * transfered before a successful authentication; non-<code>null</code> when accessed from
     * within an {@link ISpaceFilter} after successful authentication.
     */
    public AuthenticationToken getAuthenticationToken() {
        return authenticationToken;
    }

    /**
     * @return the auditDetails (may be <code>null</code>); <code>null</code> when context is
     * transfered after a successful authentication; non-<code>null</code> when accessed from within
     * an {@link ISpaceFilter}.
     */
    public AuditDetails getAuditDetails() {
        return auditDetails;
    }

    /*
     * @see java.io.Externalizable#readExternal(java.io.ObjectInput)
     */
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        byte flags = in.readByte();

        if ((flags & BitMap.AUTHENTICATION_TOKEN) != 0) {// AUTHENTICATION_TOKEN not a null
            authenticationToken = new AuthenticationToken();
            authenticationToken.readExternal(in);
        }

        if ((flags & BitMap.USER_DETAILS) != 0) { // USER_DETAILS not a null
            int length = in.readInt();
            byte[] encrypted = new byte[length];
            in.readFully(encrypted);
            //construct decoder - not concurrent, do not make static!
            AesContentEncoder contentEncoder = new AesContentEncoder(secretKey);
            try {
                userDetails = (UserDetails) contentEncoder.decode(encrypted);
            } catch (EncodingException e) {
                throw new SecurityException(
                        "Failed to decode user details; check that both client and server are using the same keystore file.",
                        e);
            }

            //sent only with user-details
            if ((flags & BitMap.AUDIT_DETAILS) != 0) { // AUDIT_DETAILS not a null
                auditDetails = new AuditDetails();
                auditDetails.readExternal(in);
            }
        }
    }

    /*
     * @see java.io.Externalizable#writeEbuildFlagsxternal(java.io.ObjectOutput)
     */
    public void writeExternal(ObjectOutput out) throws IOException {
        byte flags = buildFlags();
        out.writeByte(flags);

        if (authenticationToken != null) {
            authenticationToken.writeExternal(out);
        }

        if (userDetails != null) {
            //construct encoder - not concurrent, do not make static!
            AesContentEncoder contentEncoder = new AesContentEncoder(secretKey);
            byte[] encoded = contentEncoder.encode(userDetails);
            out.writeInt(encoded.length);
            out.write(encoded);

            //sent only with user-details
            if (auditDetails != null) {
                auditDetails.writeExternal(out);
            }
        }
    }

    /**
     * Bit map used by the serialization to avoid multi-boolean serialization
     */
    private interface BitMap {
        int AUTHENTICATION_TOKEN = 1 << 0;
        int USER_DETAILS = 1 << 1;
        int AUDIT_DETAILS = 1 << 2;
    }

    private byte buildFlags() {
        byte flags = 0;

        if (authenticationToken != null)
            flags |= BitMap.AUTHENTICATION_TOKEN;
        if (userDetails != null)
            flags |= BitMap.USER_DETAILS;
        if (auditDetails != null)
            flags |= BitMap.AUDIT_DETAILS;

        return flags;
    }
}
