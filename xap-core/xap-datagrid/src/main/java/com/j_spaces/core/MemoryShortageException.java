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


package com.j_spaces.core;

import com.gigaspaces.client.ResourceCapacityExceededException;


/**
 * This Exception indicates that the space server process reached the predefined percentage usage
 * ratio.
 */

public class MemoryShortageException
        extends ResourceCapacityExceededException {

    /** */
    private static final long serialVersionUID = 1L;

    private final String _spaceName;
    private final String _containerName;
    private final String _hostName;
    private final long _memoryUsage;

    private final long _maxMemory;

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
    public MemoryShortageException(String spaceName, String containerName, String hostName, long memoryUsage, long maxMemory) {
        super("Memory shortage at: host: " + hostName + ", container: " + containerName + ", space " + spaceName + ", total memory: " + (maxMemory / (1024 * 1024)) + " mb, used memory: " + (memoryUsage / (1024 * 1024)) + " mb");
        this._spaceName = spaceName;
        this._containerName = containerName;
        this._hostName = hostName;
        this._memoryUsage = memoryUsage;
        this._maxMemory = maxMemory;
    }

    public MemoryShortageException(String msg,String spaceName, String containerName, String hostName, long memoryUsage, long maxMemory) {
        super(msg);
        this._spaceName = spaceName;
        this._containerName = containerName;
        this._hostName = hostName;
        this._memoryUsage = memoryUsage;
        this._maxMemory = maxMemory;
    }

    /**
     * @return the name of the space that caused this exception
     */
    public String getSpaceName() {
        return this._spaceName;
    }

    /**
     * @return the name of the container that contains the space that caused this exception.
     */
    public String getContainerName() {
        return this._containerName;
    }

    /**
     * @return the name of the machine that hosts the space that caused this exception
     */
    public String getHostName() {
        return _hostName;
    }

    /**
     * @return the current memory usage
     */
    public long getMemoryUsage() {
        return _memoryUsage;
    }

    /**
     * @return the maximum amount of memory that can be used
     */
    public long getMaxMemory() {
        return _maxMemory;
    }

    /**
     * Return the exception message
     *
     * @return the exception message.
     * @see java.lang.Throwable#getMessage()
     */
    @Override
    public String toString() {
        return getMessage();
    }
}