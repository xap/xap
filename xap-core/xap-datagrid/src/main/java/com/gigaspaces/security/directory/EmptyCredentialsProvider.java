package com.gigaspaces.security.directory;

/**
 * Represents a CredentialsProvider with null UserDetails.
 * usage: {@code EmptyCredentialsProvider.get();}
 * @Since 14.0
 */
public class EmptyCredentialsProvider extends CredentialsProvider {
    private static final long serialVersionUID = -7329291946468431770L;
    private static final EmptyCredentialsProvider singleton = new EmptyCredentialsProvider();

    //private constructor to avoid client applications to use constructor
    private EmptyCredentialsProvider(){}

    @Override
    public UserDetails getUserDetails() {
        return null;
    }

    /**
     * @return a singleton instance of this class
     */
    public static EmptyCredentialsProvider get() {
        return singleton;
    }
}
