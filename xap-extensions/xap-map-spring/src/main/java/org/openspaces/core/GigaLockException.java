package org.openspaces.core;

public class GigaLockException extends RuntimeException {
    public GigaLockException(String msg, Throwable t){super(msg,t);}
    public GigaLockException(String msg){super(msg);}
}
