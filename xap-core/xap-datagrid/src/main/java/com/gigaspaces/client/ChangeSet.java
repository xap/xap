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

import com.gigaspaces.client.mutators.SpaceEntryMutator;
import com.gigaspaces.internal.client.mutators.AddAllToCollectionSpaceEntryMutator;
import com.gigaspaces.internal.client.mutators.AddToCollectionSpaceEntryMutator;
import com.gigaspaces.internal.client.mutators.IncrementSpaceEntryMutator;
import com.gigaspaces.internal.client.mutators.PutInMapSpaceEntryMutator;
import com.gigaspaces.internal.client.mutators.RemoveFromCollectionSpaceEntryMutator;
import com.gigaspaces.internal.client.mutators.RemoveFromMapSpaceEntryMutator;
import com.gigaspaces.internal.client.mutators.SetValueSpaceEntryMutator;
import com.gigaspaces.internal.client.mutators.UnsetValueSpaceEntryMutator;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.utils.Textualizable;
import com.gigaspaces.internal.utils.Textualizer;
import com.gigaspaces.serialization.SmartExternalizable;
import com.gigaspaces.server.MutableServerEntry;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;

/**
 * The set of changes to apply for matches entries of the change operation
 *
 * @author Niv Ingberg
 * @since 9.1
 */

public class ChangeSet implements SmartExternalizable, Textualizable {
    private static final long serialVersionUID = 1L;

    private final Collection<SpaceEntryMutator> _mutators;

    private long _lease;

    private transient Set<String> _nonCachedPaths = Collections.emptySet();

    /**
     * Constructs an empty change set
     */
    public ChangeSet() {
        _mutators = new LinkedList<SpaceEntryMutator>();
        _lease = 0;
    }

    ChangeSet(Collection<SpaceEntryMutator> mutators, long lease) {
        _mutators = mutators;
        _lease = lease;
    }

    private ChangeSet add(SpaceEntryMutator mutator) {
        _mutators.add(mutator);
        return this;
    }

    /**
     * Returns true if the change set is empty, i.e. does not contain any change operations
     * @since 14.0
     */
    public boolean isEmpty() {
        return _mutators.isEmpty() && _lease == 0;
    }

    /**
     * Adds a custom change operation to be executed. Note that when using a replicated topology
     * (e.g. backup space, gateway, mirror) the change operation itself is replicated (and *NOT* the
     * modified entry). Hence, it is imperative that this method will always cause the exact same
     * affect on the entry assuming the same entry was provided, for example it should not rely on
     * variables that may change between executions, such as system time, random, machine name etc.
     * If the operation is not structured that way, the state can be inconsistent in the different
     * locations after being replicated ("With great power comes great responsibility"). In a
     * secured space, unlike built-in change operations, the security privilege required for custom
     * change operation is EXECUTE instead of WRITE.
     */
    public ChangeSet custom(SpaceEntryMutator changeOperation) {
        return add(changeOperation);
    }

    public ChangeSet custom(String name, ChangeFunction<MutableServerEntry, Object> operation) {
        return custom(CustomChangeOperation.from(name, operation));
    }

    /**
     * Sets the value of the given path.
     *
     * @param path  The path that needs its value set.
     * @param value The value to set for the given path.
     */
    public ChangeSet set(String path, Serializable value) {
        return add(new SetValueSpaceEntryMutator(path, value));
    }

    /**
     * Unsets the specified path. If the path points to a dynamic property or a map key it will be
     * removed, if it points to a fixed property it will be set to null, in that case, the target
     * path must point to a nullable property otherwise an error will occur.
     *
     * @param path Path pointing to the requested property.
     */
    public ChangeSet unset(String path) {
        return add(new UnsetValueSpaceEntryMutator(path));
    }

    /**
     * Increment a byte property with the given delta.
     *
     * @param path  The path pointing to a numeric property that needs its value increased.
     * @param delta The delta to increment by.
     */
    public ChangeSet increment(String path, byte delta) {
        return add(new IncrementSpaceEntryMutator(path, delta));
    }

    /**
     * Increment a short property with the given delta.
     *
     * @param path  The path pointing to a numeric property that needs its value increased.
     * @param delta The delta to increment by.
     */
    public ChangeSet increment(String path, short delta) {
        return add(new IncrementSpaceEntryMutator(path, delta));
    }

    /**
     * Increment a integer property with the given delta.
     *
     * @param path  The path pointing to a numeric property that needs its value increased.
     * @param delta The delta to increment by.
     */
    public ChangeSet increment(String path, int delta) {
        return add(new IncrementSpaceEntryMutator(path, delta));
    }

    /**
     * Increment a long property with the given delta.
     *
     * @param path  The path pointing to a numeric property that needs its value increased.
     * @param delta The delta to increment by.
     */
    public ChangeSet increment(String path, long delta) {
        return add(new IncrementSpaceEntryMutator(path, delta));
    }

    /**
     * Increment a float property with the given delta.
     *
     * @param path  The path pointing to a numeric property that needs its value increased.
     * @param delta The delta to increment by.
     */
    public ChangeSet increment(String path, float delta) {
        return add(new IncrementSpaceEntryMutator(path, delta));
    }

    /**
     * Increment a double property with the given delta.
     *
     * @param path  The path pointing to a numeric property that needs its value increased.
     * @param delta The delta to increment by.
     */
    public ChangeSet increment(String path, double delta) {
        return add(new IncrementSpaceEntryMutator(path, delta));
    }

    /**
     * Increment a numeric property with the given delta.
     *
     * @param path  The path pointing to a numeric property that needs its value increased.
     * @param delta The delta to increment by.
     */
    public ChangeSet increment(String path, Number delta) {
        return add(new IncrementSpaceEntryMutator(path, delta));
    }

    /**
     * Decrement a byte property with the given delta.
     *
     * @param path  The path pointing to a numeric property that needs its value decreased.
     * @param delta The delta to decrement by.
     */
    public ChangeSet decrement(String path, byte delta) {
        return increment(path, -delta);
    }

    /**
     * Decrement a short property with the given delta.
     *
     * @param path  The path pointing to a numeric property that needs its value decreased.
     * @param delta The delta to decrement by.
     */
    public ChangeSet decrement(String path, short delta) {
        return increment(path, -delta);
    }

    /**
     * Decrement a integer property with the given delta.
     *
     * @param path  The path pointing to a numeric property that needs its value decreased.
     * @param delta The delta to decrement by.
     */
    public ChangeSet decrement(String path, int delta) {
        return increment(path, -delta);
    }

    /**
     * Decrement a long property with the given delta.
     *
     * @param path  The path pointing to a numeric property that needs its value decreased.
     * @param delta The delta to decrement by.
     */
    public ChangeSet decrement(String path, long delta) {
        return increment(path, -delta);
    }

    /**
     * Decrement a float property with the given delta.
     *
     * @param path  The path pointing to a numeric property that needs its value decreased.
     * @param delta The delta to decrement by.
     */
    public ChangeSet decrement(String path, float delta) {
        return increment(path, -delta);
    }

    /**
     * Decrement a double property with the given delta.
     *
     * @param path  The path pointing to a numeric property that needs its value decreased.
     * @param delta The delta to decrement by.
     */
    public ChangeSet decrement(String path, double delta) {
        return increment(path, -delta);
    }

    /**
     * Adds the given item to a collection property.
     *
     * @param path    The path pointing to the collection that the item should be added to.
     * @param newItem The item to add to the collection.
     */
    public ChangeSet addToCollection(String path, Serializable newItem) {
        return add(new AddToCollectionSpaceEntryMutator(path, newItem));
    }

    /**
     * Adds the given items to a collection property
     *
     * @param path     The path pointing to the collection that the item should be added to.
     * @param newItems The items to add to the collection.
     */
    public ChangeSet addAllToCollection(String path, Serializable... newItems) {
        return addAllToCollection(path, Arrays.asList(newItems));
    }

    /**
     * Adds the given items to a collection property
     *
     * @param path     The path pointing to the collection that the item should be added to.
     * @param newItems The items to add to the collection.
     */
    public ChangeSet addAllToCollection(String path, Collection<? extends Serializable> newItems) {
        return add(new AddAllToCollectionSpaceEntryMutator(path, newItems));
    }

    /**
     * Removes the given item from a collection property.
     *
     * @param path         The path pointing to the collection that the item should be removed
     *                     from.
     * @param itemToRemove The item to remove from the collection.
     */
    public ChangeSet removeFromCollection(String path, Serializable itemToRemove) {
        return add(new RemoveFromCollectionSpaceEntryMutator(path, itemToRemove));
    }

    /**
     * Puts the given key and value in a map property.
     *
     * @param path  The path pointing to the map that the key and value should be put into
     * @param key   The map's key.
     * @param value The value to associate with the given key.
     */
    public ChangeSet putInMap(String path, Serializable key, Serializable value) {
        return add(new PutInMapSpaceEntryMutator(path, key, value));
    }

    /**
     * Removes the given key from a map property.
     *
     * @param path The path pointing to the map that the key should be removed from.
     * @param key  The map's key.
     */
    public ChangeSet removeFromMap(String path, Serializable key) {
        return add(new RemoveFromMapSpaceEntryMutator(path, key));
    }

    /**
     * Change the entry lease with the new lease.
     *
     * @param lease Change the lease duration with the new lease, the lease specifies how much time
     *              the lease of a changed entry should be after the change operation takes affect
     *              in milliseconds.
     */
    public ChangeSet lease(long lease) {
        _lease = lease;
        return this;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(_mutators.size());
        for (SpaceEntryMutator mutator : _mutators)
            IOUtils.writeObject(out, mutator);
        out.writeLong(_lease);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            SpaceEntryMutator mutator = IOUtils.readObject(in);
            _mutators.add(mutator);
        }
        _lease = in.readLong();
    }

    @Override
    public String toString() {
        return Textualizer.toString(this);
    }

    final Collection<SpaceEntryMutator> getMutators() {
        return _mutators;
    }

    final long getLease() {
        return _lease;
    }

    @Override
    public void toText(Textualizer textualizer) {
        textualizer.appendIterable("mutators", _mutators);

    }

    /**
     * Since 14.2.0
     */
    public ChangeSet disablePathCaching(String... nonCachedPaths){
        this._nonCachedPaths = nonCachedPaths == null ? Collections.emptySet()
                                : new HashSet<>(Arrays.asList(nonCachedPaths ) );
        return this;
    }

    /**
     * Since 14.2.0
     * @return
     */
    Set<String> getNonCachedPaths(){
        return _nonCachedPaths;
    }
}