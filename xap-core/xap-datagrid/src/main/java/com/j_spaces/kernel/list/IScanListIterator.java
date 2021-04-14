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

/*
 * @(#)IScanListIterator.java   10/02/2011
 *
 * Copyright 2011 GigaSpaces Technologies Inc.
 */

package com.j_spaces.kernel.list;

import com.j_spaces.core.sadapter.SAException;

/**
 * Title:        The J-Spaces Platform Description:  IScanListIterator interface. Copyright:
 * Copyright (c) J-Spaces Team Company:      J-Spaces Technologies
 *
 * @author Yechiel Fefer
 * @version 1.0
 */

/**
 * IScanListIterator defines the interface for the result of stored-lists/extended indexes matching
 */

public interface IScanListIterator<T>
        extends IObjectsList {

    boolean hasNext() throws SAException;

    T next() throws SAException;

    /**
     * release SLHolder for this scan
     */
    void releaseScan() throws SAException;

    /**
     * if the scan is on a property index, currently supported for extended index
     */
    int getAlreadyMatchedFixedPropertyIndexPos();

    default String getAlreadyMatchedIndexPath() { return null; }

    /**
     * is the entry returned already matched against the searching template currently is true if the
     * underlying scan made by CacheManager::EntriesIter
     */
    boolean isAlreadyMatched();

    /**
     * @return true if this iterator is QueryExtensionIndexEntryIteratorWrapper
     */
    default boolean isExtensionIndex() { return false; }

    /**
     * does this iter contain multiple lists
     */
    default boolean isMultiListsIterator()
    {
        return false;
    }
    default boolean noRematchNeeded()
    {
        return false;
    }

    /**
     * create a shallow copy ready for alternating thread usage
     * @return a new shallow copyed IScanListIterator ready for alternating thread usage
     * NOTE!! should be called before first hasNext() call
     */
    default IScanListIterator createCopyForAlternatingThread()
    {
        throw new RuntimeException("internal error-invalid usage");
    }

    /**
     * @return true if the iterator knows its size, and size() can be called.
     */
    default boolean hasSize() { return false;}
    default int size() { return Integer.MAX_VALUE;}
}
