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

package com.j_spaces.core.client;

import com.gigaspaces.annotation.pojo.SpaceFifoGroupingProperty;
import com.gigaspaces.client.ReadMultipleException;
import com.gigaspaces.client.TakeMultipleException;
import com.gigaspaces.document.SpaceDocument;

/**
 * The Modifier class provides <code>static</code> methods and constants to decode available
 * GigaSpaces types modifiers.  The sets of modifiers are represented as integers with distinct bit
 * positions representing different modifiers.
 *
 * @author Igor Goldenberg
 * @version 1.0
 * @since 5.1
 **/
@com.gigaspaces.api.InternalApi
public class Modifiers {
    /**
     * This modifier represents NONE modifier without any bit flag.
     **/
    public static final int NONE = 0;

    /**
     * This modifier is relevant only for remote space. If set, using this modifier the client side
     * will not be blocked while space' operation will be processed. The client thread will be
     * released right away when the method request-call is transfered to the server. Notice: The
     * return value is always <code>null</code>. Operations: Any
     **/
    public static final int ONE_WAY = 1 << 0;

    /**
     * If set, the write or update operations will return <code>null</code> instead of the lease
     * object (write) or previous value (update). Using this option will improve application write
     * operation performance - null return value yields less network traffic and less memory
     * consumption. Note: This modifier does not replace ONE_WAY modifier
     *
     * @deprecated Since 9.0.0
     **/
    @Deprecated
    public static final int NO_RETURN_VALUE = 1 << 1;

    /**
     * Performs a regular write operation. Operations: Write
     **/
    public static final int WRITE = 1 << 2;

    /**
     * If set, performs a regular update operation. In case the Entry UID doesn't exists in the
     * space use the following modifier combination: UPDATE | WRITE | update the entry if exists or
     * write if absent. Operations: Update
     **/
    public static final int UPDATE = 1 << 3;

    /**
     * If set, null values in the updated entry are treated as "leave as it is". Operations: Update
     **/
    public static final int PARTIAL_UPDATE = 1 << 4;

    /**
     * If set, the Dirty read option allows you to retrieve the latest state of the object before it
     * has been committed. Operations: Read, Take, Update
     **/
    public static final int DIRTY_READ = 1 << 5;

    /**
     * Indicates operation should run directly on i/o thread, instead of being submitted to the operations thread pool.
     * This is useful for short, non-blocking operations (e.g. readById), but risky for long or non-blocking operations.
     * NOTE: This is an experimental API, subject to breaking changes in future releases.
     * @since 16.0
     */
    public static final int RUN_ON_IO_THREAD = 1 << 6;

    /**
     * If set, The match Collection will contain an instance of Entry UID of read or take
     * operations. If set for write operation, the return value will be the written UID and not the
     * Lease instance. Operations: Write, Read, Take
     **/
    public static final int RESERVED_RETURN_ONLY_UID = 1 << 7;

    /**
     * Read any matching object from the space, returning <code>null</code> if there currently is
     * none. Matching and timeouts are done as in <code>read</code>, except that blocking in this
     * call is done only if necessary to wait for transactional state to settle. Operations: Read,
     * Take
     *
     * @since 10.0
     */
    public static final int IF_EXISTS = 1 << 8;

    /**
     * Provides an Exclusive Read lock.
     */
    public static final int EXCLUSIVE_READ_LOCK = 1 << 9;

    /**
     * An indication whether the desired operation received from <code>java.io.Externalizable</code>
     * operations. Operations: Any
     **/
    public static final int RESERVED_EXTERNALIZABLE_TRANSPORT = 1 << 10;

    /**
     * An internal usage to get indication whether the desired operation represents FIFO.
     * Operations: Any
     **/
    public static final int FIFO = 1 << 11;

    /**
     * The <code>int</code> value representing the <code>UPDATE_OR_WRITE</code> modifier. When set:
     * if the entry to be updated does not reside in the space, a new entry is written <p>
     */
    public static final int UPDATE_OR_WRITE = 1 << 12;

    /**
     * If set, provides read_committed isolation per the SPECIFIC read operation read_committed can
     * also set , in the traditional way, per created transactions using the proxy level
     * setTransactionIsolationLevel() API effected operations: Operations: Read, readIfExists,
     * readMultiple
     **/
    public static final int READ_COMMITTED = 1 << 13;

    /**
     * The <code>int</code> value required matching to be done only by UID if provided (not in POJO)
     * modifier.
     */
    public static final int MATCH_BY_ID = 1 << 14;

    /**
     * A modifier passed to read multiple and take multiple operations. The modifier will cause an
     * operation specific exception {@link ReadMultipleException},{@link TakeMultipleException}  to
     * be thrown when not all of the requested number of entries are returned and one or more
     * cluster members are not available.
     *
     * @deprecated This is the default behavior, for old behavior use {@link
     * #IGNORE_PARTIAL_FAILURE}
     */
    @Deprecated
    public static final int THROW_PARTIAL_FAILURE = 1 << 15;

    /**
     * A modifier passed to read multiple and take multiple operations. The modifier will cause
     * partial results to be returned instead of throwing operation specific exception {@link
     * ReadMultipleException},{@link TakeMultipleException} when not all of the requested number of
     * entries are returned and one or more cluster members are not available.
     *
     * @since 7.1
     */
    public static final int IGNORE_PARTIAL_FAILURE = 1 << 16;

    /**
     * A modifier passed to take operations. The modifier will cause eviction from cache of the
     * selected entries- relevant for LRU cache policy only
     *
     * @since 8.0
     */
    public static final int EVICT_ONLY = 1 << 17;

    /**
     * Deprecated since 9.0.0.
     */
    @Deprecated
    public static final int NO_WRITE_LEASE = 1 << 18;

    /**
     * For internal use.
     */
    public static final int OVERRIDE_VERSION = 1 << 19;

    /**
     * A modifier passed to take & read operations. The modifier will cause a group fifo poll based
     * on the passed template. The template must have a property marked with the {@link
     * SpaceFifoGroupingProperty}
     *
     * @since 9.0.0
     */
    public static final int FIFO_GROUPING_POLL = 1 << 20;

    /**
     * Internal property. Used mostly by the data console to fetch data without loading the data
     * classes.
     *
     * @since 9.0.0
     */
    public static final int RETURN_STRING_PROPERTIES = 1 << 21;

    /**
     * Internal property. Used mostly by the data console to fetch data as {@link SpaceDocument},
     * without loading the data classes.
     *
     * @since 9.0.0
     */
    public static final int RETURN_DOCUMENT_PROPERTIES = 1 << 22;

    /**
     * Determines if an update operation should return the previous entry in the lease.
     *
     * @since 9.0.1
     */
    public static final int RETURN_PREV_ON_UPDATE = 1 << 23;

    /**
     * Search for proper entries in cache memory only (dont use the underlying EDS).
     *
     * @since 9.0.1
     */
    public static final int MEMORY_ONLY_SEARCH = 1 << 24;

    /**
     * Determines if a change operation should return detailed results.
     *
     * @since 9.1
     */
    public static final int RETURN_DETAILED_CHANGE_RESULT = 1 << 25;

    public static final int LOG_SCANNED_ENTRIES_COUNT = 1 << 26;

    /**
     * Determines if an update operation notification should return the previous entry.
     *
     * @since 10.1
     */
    public static final int RETURN_PREV_ON_UPDATE_NOTIFY = 1 << 27;

    /**
     * Determines if an update operation notification should return the previous entry.
     *
     * @since 12.0.1
     */
    public static final int EXPLAIN_PLAN = 1 << 28;

    /**
     * Internal API
     * Determines if an update operation should be replicated only to backup
     * (only write and take supported, only write and clear tested)
     *  used in horizontal scale feature
     *
     * @since 15.5.0
     */
    public static final int BACKUP_ONLY = 1 << 29;

    /**
     * Allows the query to fill the Explain plan without returning results and scanning the indexes
     * @since 16.0
     */
    public static final int DRY_RUN = 1 << 30;

    public static boolean contains(int modifiers, int setting) {
        return (modifiers & setting) != 0;
    }

    public static int add(int modifiers, int setting) {
        return modifiers | setting;
    }

    public static int remove(int modifiers, int setting) {
        return modifiers & ~setting;
    }
}