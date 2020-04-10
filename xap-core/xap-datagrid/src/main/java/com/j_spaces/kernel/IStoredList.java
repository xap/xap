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
 * Title:		IStoredList.java
 * Description:	
 * Company:		GigaSpaces Technologies
 * 
 * @author      Yechiel Fefer
 * @author		Moran Avigdor
 * @version		1.0 20/06/2005
 * @since		4.1 Build#1191
 */
package com.j_spaces.kernel;

import com.gigaspaces.internal.utils.collections.economy.IHashEntry;
import com.j_spaces.kernel.list.IObjectsList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 */
public interface IStoredList<T> extends ICollection<T>,
//	IHashEntry<Object, IStoredList<T>>
        IHashEntry<Object, IStoredList<T>>, IObjectsList

{


    /**
     * Returns the first element in the list (fifo)
     *
     * @return the head of the list
     */
    public abstract IObjectInfo<T> getHead();

    /**
     * Returns the value of the first element in the list (fifo)
     *
     * @return the value of the head of the list
     */
    public abstract T getObjectFromHead();

    /**
     * return true if we can save iterator creation and get a single entry
     *
     * @return true if we can optimize
     */
    public abstract boolean optimizeScanForSingleObject();

    /**
     * Called by an outside scan that wants to quit the scan and return the slholder to the resource
     * pool.
     *
     * @param slh Holder resource to return to pool; can be <code>null</code>.
     */
    public abstract void freeSLHolder(IStoredListIterator<T> slh);

    /**
     * Remove an element described by ObjectInfo.
     *
     * [Tail] - .. - [beforeOi] - [oi] - [afterOi] - .. - [Head]
     *
     * @param oi an existing element between Tail and Head
     */
    public abstract void remove(IObjectInfo<T> oi);

    /**
     * Remove an element described by ObjectInfo, while the SL is unlocked.
     *
     * [Tail] - .. - [beforeOi] - [oi] - [afterOi] - .. - [Head]
     *
     * @param oi an existing element between Tail and Head
     */
    public abstract void removeUnlocked(IObjectInfo<T> oi);

    /**
     * Store an element in the list, while the SL is unlocked . New elements are inserted at the
     * tail.
     *
     * @param subject element to store.
     * @return an information node representing the inserted element; <code>null</code> if
     * StoredList has been invalidated.
     */
    public abstract IObjectInfo<T> addUnlocked(T subject);


    /**
     * Sets an indication that this StoredList is invalid.
     *
     * if {@linkplain #isEmpty() isEmpty()} returns true, the indication is set; otherwise the
     * indication remains false.
     *
     * Called by {@linkplain com.j_spaces.core.cache.PersistentGC PersistentGC} when scanning for
     * empty StoredList that can be garbage collected.
     *
     * @return <code>true</code> if StoredList was set to invalid; <code>false</code> otherwise.
     */
    public abstract boolean invalidate();


    /**
     * Dump list content, used for Debug.
     *
     * @param logger logger to use
     * @param msg    message to add in log
     */
    public void dump(Logger logger, String msg);
}
