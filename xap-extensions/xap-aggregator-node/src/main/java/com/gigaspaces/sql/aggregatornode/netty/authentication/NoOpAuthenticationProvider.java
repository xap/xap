package com.gigaspaces.sql.aggregatornode.netty.authentication;

public class NoOpAuthenticationProvider implements AuthenticationProvider{
    @Override
    public Authentication authenticate(Authentication authentication) {
        return Authentication.OK;
    }
}
