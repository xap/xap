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

//
package com.j_spaces.core.cache.blobStore;

import com.gigaspaces.server.blobstore.BlobStoreException;

/**
 * Exception class that is thrown when BlobStore crossed the designated memory threshold . This
 * class extends <code>RuntimeException</code>
 *
 * @author Kobi
 * @since 10.1.0
 */
@com.gigaspaces.api.InternalApi
public class BlobStoreMemoryShortageException extends BlobStoreException {
    private final long _memoryUsage;

    private final long _maxMemory;

    private static final long serialVersionUID = 1L;

    public BlobStoreMemoryShortageException(String message, long memoryUsage, long maxMemory) {
        super(message);
        this._memoryUsage = memoryUsage;
        this._maxMemory = maxMemory;
    }

    public BlobStoreMemoryShortageException(Throwable cause, long memoryUsage, long maxMemory) {
        super(cause);
        this._memoryUsage = memoryUsage;
        this._maxMemory = maxMemory;
    }

    public BlobStoreMemoryShortageException(String message, Throwable cause, long memoryUsage, long maxMemory) {
        super(message, cause);
        this._memoryUsage = memoryUsage;
        this._maxMemory = maxMemory;
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
}
