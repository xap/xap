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

/**
 *
 */
package com.j_spaces.kernel;


/**
 * Interface for all internal collections.
 *
 * @author anna
 * @see IStoredList
 * @see IOrderedList
 * @since 6.1
 */
public interface ICollection<T> {

    /**
     * Returns the amount of elements in the StoredList.
     *
     * @return size of the StoredList
     * @see #isEmpty()
     */
    public int size();

    /**
     * Returns true if this list is empty, i.e. has no elements. [Tail] - [Head]
     *
     * @return <code>true</code> if list is empty; <code>false</code> otherwise.
     */
    public boolean isEmpty();

    /**
     * establish a scan position. if random scan is supported we select a random position; otherwise
     * we return the first resource by scanning from head to tail.
     *
     * @param random_scan <code>true</code> enable random scan; <code>false</code> start from head.
     * @return a  Holder for this scan. This Resource should be released if scan ended prematurely.
     */
    public abstract IStoredListIterator<T> establishListScan(boolean random_scan);

    /**
     * Returns the next element in the scan order. Calling this method until it returns null, will
     * return each element as part of the <code>SLHolder</code>s <tt>subject</tt>.
     *
     * @param slh Holder representing a pivot position.
     * @return the next pivot position in this scan; <code>null</code> if reached end of scan.
     */
    public abstract IStoredListIterator<T> next(IStoredListIterator<T> slh);

    /**
     * Store an element in the list. New elements are inserted at the tail.
     *
     * @param subject element to store.
     * @return an information node representing the inserted element; <code>null</code> if
     * StoredList has been invalidated.
     */
    public abstract IObjectInfo<T> add(T subject);

    /**
     * Given an element scan the list, find it and remove it.
     *
     * @param obj element to remove
     * @return <code>true</code> if object was removed; <code>false</code> otherwise (if element
     * wasn't found)
     */
    public abstract boolean removeByObject(T obj);

    /**
     * Checks whether it contains the specified element.
     *
     * @param obj the element to search for
     * @return <code>true</code> if element exists in the StoredList; <code>false</code> otherwise.
     */
    public abstract boolean contains(T obj);

    /**
     * is it an actual multi object container.
     *
     * @return <code>true</code> if its an actual list; <code>false</code> otherwise
     */
    public abstract boolean isMultiObjectCollection();

}