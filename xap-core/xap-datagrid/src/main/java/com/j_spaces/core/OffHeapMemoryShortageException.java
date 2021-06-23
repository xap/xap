package com.j_spaces.core;

/**
 * @author Rotem Herzberg
 * @since 12.3
 */
public class OffHeapMemoryShortageException extends MemoryShortageException {
    private static final long serialVersionUID = -7899267111128359170L;
    /**
     * Constructor
     *
     * @param spaceName     the name of the space that caused this exception
     * @param containerName the name of the container that contains the space that caused this
     *                      exception.
     * @param hostName      the name of the machine that hosts the space that caused this exception
     * @param memoryUsage   the amount of memory in use
     * @param maxMemory     the maximum amount of memory that can be used
     */
    public OffHeapMemoryShortageException(String msg,String spaceName, String containerName, String hostName, long memoryUsage, long maxMemory) {
        super(msg ,spaceName, containerName, hostName, memoryUsage, maxMemory);
    }
}
