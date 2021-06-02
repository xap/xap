package com.gigaspaces.sql.aggregatornode.netty.authentication;

public class ClearTextPassword extends Authentication{
    private final String password;

    public ClearTextPassword(String password) {
        this.password = password;
    }

    public String getPassword() {
        return this.password;
    }
}
