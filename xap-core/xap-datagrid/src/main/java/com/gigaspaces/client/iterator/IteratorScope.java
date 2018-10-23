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


package com.gigaspaces.client.iterator;

/**
 * Determines the scope of a GSIterator.
 *
 * @author niv
 * @since 7.0
 * @deprecated Since 14.0
 */
@Deprecated
public enum IteratorScope {
    /**
     * Indicates that the iterator will process entries currently in the space, and ignores future
     * changes.
     */
    CURRENT,
    /**
     * Indicates that the iterator will ignore entries currently in the space, and process future
     * changes.
     */
    FUTURE,
    /**
     * Indicates that the iterator will process both entries currently in the space and future
     * changes.
     */
    CURRENT_AND_FUTURE;

    /**
     * @return <code>true<code> if the value is {@link #CURRENT} or {@link #CURRENT_AND_FUTURE},
     * <code>false</code> otherwise.
     */
    public boolean hasCurrent() {
        return this == CURRENT || this == CURRENT_AND_FUTURE;
    }

    /**
     * @return true if the value is {@link #FUTURE} or {@link #CURRENT_AND_FUTURE}, false otherwise.
     */
    public boolean hasFuture() {
        return this == FUTURE || this == CURRENT_AND_FUTURE;
    }
}
