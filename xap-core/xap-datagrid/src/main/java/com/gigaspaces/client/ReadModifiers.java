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


package com.gigaspaces.client;

import com.gigaspaces.annotation.pojo.SpaceFifoGroupingProperty;
import com.gigaspaces.api.ExperimentalApi;
import com.gigaspaces.internal.utils.collections.CopyOnUpdateOnceMap;
import com.j_spaces.core.client.Modifiers;

import java.util.Map;

/**
 * Provides modifiers to customize the behavior of read operations.
 *
 * @author Niv Ingberg
 * @since 9.0.1
 */

public class ReadModifiers extends IsolationLevelModifiers {
    private static final long serialVersionUID = -5120634891611462402L;

    /**
     * Empty - use operation default behavior.
     */
    public static final ReadModifiers NONE = new ReadModifiers(Modifiers.NONE);

    /**
     * When a search for matching entries encounters an entry locked under a different uncommitted
     * transaction, the lock is honored and the entry is skipped (if the operation is invoked with a
     * timeout it will block until the entry will be released or the timeout expires). If the
     * operation is invoked in a transactional context, the resulting entries will be locked,
     * blocking update/remove from other transactions.
     */
    public static final ReadModifiers REPEATABLE_READ = new ReadModifiers(Modifiers.NONE);

    /**
     * When a search for matching entries encounters an entry locked under a different uncommitted
     * transaction, the lock is honored and matching is performed on the committed state.
     */
    public static final ReadModifiers READ_COMMITTED = new ReadModifiers(Modifiers.READ_COMMITTED);

    /**
     * When a search for matching entries encounters an entry locked under a different uncommitted
     * transaction, the lock is ignored and matching is performed on the uncommitted state.
     */
    public static final ReadModifiers DIRTY_READ = new ReadModifiers(Modifiers.DIRTY_READ);

    /**
     * The resulting entries are locked, blocking update/remove from other transactions.
     */
    public static final ReadModifiers EXCLUSIVE_READ_LOCK = new ReadModifiers(Modifiers.EXCLUSIVE_READ_LOCK);

    /**
     * If one or more partitions are not available during the operation, ignore them and return
     * partial results based on the available partitions (instead of throwing an
     * ReadMultipleException which contains the partial results, which is the default behavior).
     * Relevant only for read multiple operations when the result set maximum size is not reached.
     */
    public static final ReadModifiers IGNORE_PARTIAL_FAILURE = new ReadModifiers(Modifiers.IGNORE_PARTIAL_FAILURE);

    /**
     * Search for matching entries using First-In-First-Out order.
     */
    public static final ReadModifiers FIFO = new ReadModifiers(Modifiers.FIFO);

    /**
     * Search for matching entries in the FIFO group indicated in the template. The template must
     * have a property marked with the {@link SpaceFifoGroupingProperty}.
     *
     * @see {@link SpaceFifoGroupingProperty}
     */
    public static final ReadModifiers FIFO_GROUPING_POLL = new ReadModifiers(Modifiers.FIFO_GROUPING_POLL);

    /**
     * Search for matching entries in cache memory only (do not use the underlying EDS).
     */
    public static final ReadModifiers MEMORY_ONLY_SEARCH = new ReadModifiers(Modifiers.MEMORY_ONLY_SEARCH);

    /**
     * Blocks only if there's a matching entry which is locked by another transaction. If the
     * timeout elapses and the matching entry is still locked, an {@link EntryLockedException} will
     * be thrown. Here are some of the possible cases when using this modifier: <ul> <li>If there is
     * an unlocked entry in the space that match immediately return the match entry.</li> <li>If
     * there is not entry that match immediately return null.</li> <li>If all the matches are locked
     * wait TIMEOUT for one of those entries to finish the transaction, if one of the entries finish
     * the transaction before TIMEOUT return this entry otherwise throw {@link
     * EntryLockedException}</li> <li>If while waiting for a locked entry another match entry is
     * written to space and it is not locked return this new match entry.</li> </ul>
     *
     * @since 10.0
     */
    public static final ReadModifiers IF_EXISTS = new ReadModifiers(Modifiers.IF_EXISTS);

    private static final Map<Integer, SpaceProxyOperationModifiers> cache = initCache();

    private static Map<Integer, SpaceProxyOperationModifiers> initCache() {
        Map<Integer, SpaceProxyOperationModifiers> initialValues = new CopyOnUpdateOnceMap<Integer, SpaceProxyOperationModifiers>();
        initialValues.put(NONE.getCode(), NONE);
        initialValues.put(REPEATABLE_READ.getCode(), REPEATABLE_READ);
        initialValues.put(READ_COMMITTED.getCode(), READ_COMMITTED);
        initialValues.put(DIRTY_READ.getCode(), DIRTY_READ);
        initialValues.put(EXCLUSIVE_READ_LOCK.getCode(), EXCLUSIVE_READ_LOCK);
        initialValues.put(IGNORE_PARTIAL_FAILURE.getCode(), IGNORE_PARTIAL_FAILURE);
        initialValues.put(FIFO.getCode(), FIFO);
        initialValues.put(FIFO_GROUPING_POLL.getCode(), FIFO_GROUPING_POLL);
        initialValues.put(MEMORY_ONLY_SEARCH.getCode(), MEMORY_ONLY_SEARCH);
        initialValues.put(IF_EXISTS.getCode(), IF_EXISTS);
        return initialValues;
    }

    /**
     * Required for Externalizable
     */
    public ReadModifiers() {
    }

    protected ReadModifiers(int code) {
        super(code);
    }

    /**
     * Creates a new modifiers from the specified modifiers.
     */
    public ReadModifiers(ReadModifiers modifiers1, ReadModifiers modifiers2) {
        super(modifiers1, modifiers2);
    }

    /**
     * Creates a new modifiers from the specified modifiers.
     */
    public ReadModifiers(ReadModifiers modifiers1, ReadModifiers modifiers2, ReadModifiers modifiers3) {
        super(modifiers1, modifiers2, modifiers3);
    }

    /**
     * Creates a new modifiers from the specified modifiers.
     */
    public ReadModifiers(ReadModifiers... modifiers) {
        super(modifiers);
    }

    /**
     * Checks if the specified modifier is set.
     *
     * @return true if the specified modifier is set, false otherwise.
     */
    public boolean contains(ReadModifiers modifiers) {
        return super.contains(modifiers);
    }

    /**
     * Creates a new modifiers instance which is a union of the specified modifiers and this
     * instance.
     *
     * @param modifiers Modifiers to add.
     * @return A union of the current modifiers and the specified modifiers.
     */
    public ReadModifiers add(ReadModifiers modifiers) {
        return createIfNeeded(super.add(modifiers));
    }

    /**
     * Creates a new modifiers instance which excludes the specified modifiers from this instance.
     *
     * @param modifiers Modifiers to remove.
     * @return The modifiers from this instance without the modifiers from the specified instance.
     */
    public ReadModifiers remove(ReadModifiers modifiers) {
        return createIfNeeded(super.remove(modifiers));
    }

    /**
     * Creates a new modifiers instance with the specified isolation level set and any other
     * isolation level unset.
     *
     * @param isolationLevel The isolation level to set.
     * @return The modifiers from this instance with the new isolation level set and any previous
     * one removed.
     */
    public ReadModifiers setIsolationLevel(ReadModifiers isolationLevel) {
        return super.setIsolationLevel(isolationLevel);
    }

    /**
     * Checks if this instance contains the {@link #EXCLUSIVE_READ_LOCK} setting.
     *
     * @return true if this instance contains the {@link #EXCLUSIVE_READ_LOCK} setting, false
     * otherwise.
     */
    public boolean isExclusiveReadLock() {
        return contains(EXCLUSIVE_READ_LOCK);
    }

    /**
     * Checks if this instance contains the {@link #IGNORE_PARTIAL_FAILURE} setting.
     *
     * @return true if this instance contains the {@link #IGNORE_PARTIAL_FAILURE} setting, false
     * otherwise.
     */
    public boolean isIgnorePartialFailure() {
        return contains(IGNORE_PARTIAL_FAILURE);
    }

    /**
     * Checks if this instance contains the {@link #FIFO} setting.
     *
     * @return true if this instance contains the {@link #FIFO} setting, false otherwise.
     */
    public boolean isFifo() {
        return contains(FIFO);
    }

    /**
     * Checks if this instance contains the {@link #FIFO_GROUPING_POLL} setting.
     *
     * @return true if this instance contains the {@link #FIFO_GROUPING_POLL} setting, false
     * otherwise.
     */
    public boolean isFifoGroupingPoll() {
        return contains(FIFO_GROUPING_POLL);
    }

    /**
     * Checks if this instance contains the {@link #MEMORY_ONLY_SEARCH} setting.
     *
     * @return true if this instance contains the {@link #MEMORY_ONLY_SEARCH} setting, false
     * otherwise.
     */
    public boolean isMemoryOnlySearch() {
        return contains(MEMORY_ONLY_SEARCH);
    }

    /**
     * Checks if this instance contains the {@link #IF_EXISTS} setting.
     *
     * @return true if this instance contains the {@link #IF_EXISTS} setting, false otherwise.
     */
    public boolean isIfExists() {
        return contains(IF_EXISTS);
    }

    @Override
    protected ReadModifiers create(int modifiers) {
        return new ReadModifiers(modifiers);
    }

    @Override
    protected Map<Integer, SpaceProxyOperationModifiers> getCache() {
        return cache;
    }
}
