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


package org.openspaces.core;

import com.j_spaces.core.MemoryShortageException;
import com.j_spaces.core.cache.blobStore.SpaceInternalBlobStoreMemoryShortageException;

/**
 * This Exception indicates that the space server process reached the predefined percentage usage
 * ratio. Wraps {@link com.j_spaces.core.MemoryShortageException MemoryShortageException}.
 *
 * @author kimchy
 */
public class SpaceMemoryShortageException extends ResourceCapacityExceededException {

    private static final long serialVersionUID = 7772971816257691465L;

    private MemoryShortageException e;

    public SpaceMemoryShortageException(MemoryShortageException e) {
        super(e.getMessage(), e);
        this.e = e;
    }

    public MemoryShortageException getMemoryShortageException() {
        return e;
    }

    public String getSpaceName() {
        return e.getSpaceName();
    }

    public boolean isFromBlobStore() {
        return (e instanceof SpaceInternalBlobStoreMemoryShortageException);
    }
}
