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

/**
 * @author Rotem Herzberg
 * @since 12.3
 */
public class OffHeapMemoryShortageException extends MemoryShortageException {
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
