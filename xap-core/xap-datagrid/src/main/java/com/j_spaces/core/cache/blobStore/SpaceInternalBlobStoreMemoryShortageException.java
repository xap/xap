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


package com.j_spaces.core.cache.blobStore;

import com.j_spaces.core.MemoryShortageException;

/**
 * Exception class that is thrown when BlobStore crossed the designated memory threshold . This
 * class extends <code>RuntimeException</code>
 *
 * @author Yechiel
 * @since 10.1.0
 */

public class SpaceInternalBlobStoreMemoryShortageException extends MemoryShortageException {
    private static final long serialVersionUID = 1L;

    private final Throwable _cause;

    public SpaceInternalBlobStoreMemoryShortageException(String spaceName, String containerName, String hostName, long memoryUsage, long maxMemory, BlobStoreMemoryShortageException cause) {
        super(spaceName, containerName, hostName, memoryUsage, maxMemory);
        _cause = cause;
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

    /**
     * Returns the detail message string of this throwable.
     *
     * @return the detail message string of this {@code Throwable} instance (which may be {@code
     * null}).
     */
    @Override
    public String getMessage() {
        return ("Blob-Store Memory shortage at: host: " + getHostName() + ", container: " + getContainerName() + ", space " + getSpaceName() + ", total memory: " + (getMaxMemory() / (1024 * 1024)) + " mb, used memory: " + (getMemoryUsage() / (1024 * 1024)) + " mb" + " cause=" + _cause.toString());
    }


}
