package com.gigaspaces.sql.aggregatornode.netty.authentication;

/**
 * Hides authentication internals, may use various
 * authentication backends (kerberos, LDAP, username-password, etc).
 */
public interface AuthenticationProvider {
    AuthenticationProvider NO_OP_PROVIDER = a -> Authentication.OK;

    /**
     * Authenticates a client.
     * @param authentication Client authentication.
     * @return Authentication result.
     */
    Authentication authenticate(Authentication authentication);
}
