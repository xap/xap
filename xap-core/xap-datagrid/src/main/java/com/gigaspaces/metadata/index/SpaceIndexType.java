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


package com.gigaspaces.metadata.index;

/**
 * Determines a Space index type.
 *
 * @author Niv Ingberg
 * @since 7.1
 */
public enum SpaceIndexType {
    /**
     * Not indexed.
     */
    NONE,
    /**
     * Basic index - supports equality.
     *
     * @deprecated since 12.3 - use {@link #EQUAL} instead.
     */
    @Deprecated
    BASIC,
    /**
     * Extended(ordered) index - supports comparison.
     *
     * @deprecated since 12.3 - use {@link #ORDERED} or {@link #EQUAL_AND_ORDERED} instead.
     */
    @Deprecated
    EXTENDED,
    /**
     * Index that supports equality.
     * @since 12.3
     */
    EQUAL,
    /**
     * Index that supports comparison.
     * @since 12.3
     */
    ORDERED,
    /**
     * Index that supports both equality and comparison.
     * @since 12.3
     */
    EQUAL_AND_ORDERED;

    /**
     * @return true if this index type indicates an indexed state, false otherwise.
     */
    public boolean isIndexed() {
        return this != NONE;
    }

    public boolean isOrdered(){
        return this == ORDERED || this == EQUAL_AND_ORDERED || this == EXTENDED;
    }
}
