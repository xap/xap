package com.gigaspaces.internal.os;

/**
 * @author Niv Ingberg
 * @since 14.2
 */
public interface ProcessKiller {
    String getName();
    boolean kill(long pid, long timeout, boolean recursive);
}
