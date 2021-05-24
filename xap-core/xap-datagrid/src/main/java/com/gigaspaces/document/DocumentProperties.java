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


package com.gigaspaces.document;

import com.gigaspaces.internal.collections.CollectionsFactory;
import com.gigaspaces.internal.collections.MapProcedure;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.serialization.SmartExternalizable;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * An implementation of {@link java.util.Map} optimized for usage with {@link
 * com.gigaspaces.document.SpaceDocument}.
 *
 * <code>DocumentProperties</code> contains the following enhancements: 1. Usability: Fluent-styled
 * setProperty method to enable fluent coding. 2. Serialization performance: {@link
 * java.io.Externalizable} implementation with special optimization for properties names. 3. Memory
 * footprint: Open-addressing algorithm is used to conserve memory usage.
 *
 * @author Niv Ingberg
 * @see com.gigaspaces.document.SpaceDocument
 * @since 8.0
 */
// NOTE: SmartExternalizable is not used here since DocumentProperties might contain cyclic references
public class DocumentProperties implements Map<String, Object>, Externalizable {
    private static final long serialVersionUID = 1L;

    private Map<String, Object> _map;

    /**
     * Default initial capacity.
     */
    private static final int DEFAULT_INITIAL_CAPACITY = 16;

    /**
     * The load factor used when none specified in constructor.
     **/
    private static final float DEFAULT_LOAD_FACTOR = 0.75f;

    private static final Map<String, Object> EMPTY_PROPERTIES = CollectionsFactory.getInstance().createMap();

    /**
     * Constructs a new <tt>DocumentProperties</tt>.
     */
    public DocumentProperties() {
        this(DEFAULT_INITIAL_CAPACITY, DEFAULT_LOAD_FACTOR);
    }

    /**
     * Constructs a new <tt>DocumentProperties</tt> with the specified initial capacity.
     *
     * @param initialCapacity The initial capacity.
     */
    public DocumentProperties(int initialCapacity) {
        this(initialCapacity, DEFAULT_LOAD_FACTOR);
    }

    /**
     * Constructs a new <tt>DocumentProperties</tt> with the specified initial capacity and load
     * factor.
     *
     * @param initialCapacity The initial capacity.
     * @param loadFactor      The load factor.
     */
    public DocumentProperties(int initialCapacity, float loadFactor) {
        this._map = CollectionsFactory.getInstance().createMap(initialCapacity, loadFactor);
    }

    /**
     * Constructs a new <tt>DocumentProperties</tt> with the same properties as the specified
     * <tt>Map</tt>.
     *
     * @param properties The map whose mappings are to be copied.
     */
    public DocumentProperties(Map<String, Object> properties) {
        this._map = CollectionsFactory.getInstance().createMap(properties != null ? properties : EMPTY_PROPERTIES);
    }

    /**
     * Get document property value by name
     *
     * @param name property name
     * @return the property value
     */
    @SuppressWarnings("unchecked")
    public <T> T getProperty(String name) {
        return (T) _map.get(name);
    }

    /**
     * Set document property
     *
     * @param name  property name
     * @param value property value
     */
    public DocumentProperties setProperty(String name, Object value) {
        _map.put(name, value);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    public int size() {
        return _map.size();
    }

    /**
     * {@inheritDoc}
     */
    public boolean isEmpty() {
        return _map.isEmpty();
    }

    /**
     * {@inheritDoc}
     */
    public boolean containsKey(Object key) {
        return _map.containsKey(key);
    }

    /**
     * {@inheritDoc}
     */
    public boolean containsValue(Object value) {
        return _map.containsValue(value);
    }

    /**
     * {@inheritDoc}
     */
    public Object get(Object key) {
        return _map.get(key);
    }

    /**
     * {@inheritDoc}
     */
    public Object put(String key, Object value) {
        return _map.put(key, value);
    }

    /**
     * {@inheritDoc}
     */
    public Object remove(Object key) {
        return _map.remove(key);
    }

    /**
     * {@inheritDoc}
     */
    public void putAll(Map<? extends String, ? extends Object> t) {
        _map.putAll(t);
    }

    /**
     * {@inheritDoc}
     */
    public void clear() {
        _map.clear();
    }

    /**
     * {@inheritDoc}
     */
    public Set<String> keySet() {
        return _map.keySet();
    }

    /**
     * {@inheritDoc}
     */
    public Collection<Object> values() {
        return _map.values();
    }

    /**
     * {@inheritDoc}
     */
    public Set<java.util.Map.Entry<String, Object>> entrySet() {
        return _map.entrySet();
    }

    /**
     * {@inheritDoc}
     */
    public void writeExternal(ObjectOutput out)
            throws IOException {
        final int size = _map.size();
        System.out.println("Writing DocumentProperties - size: " + size + ", id: " + System.identityHashCode(this));
        out.writeInt(size);
        if (size != 0)
            SerializationProcedure.execute(_map, out);
    }

    /**
     * {@inheritDoc}
     */
    public void readExternal(ObjectInput in)
            throws IOException, ClassNotFoundException {
        final int size = in.readInt();
        _map = CollectionsFactory.getInstance().createMap(size);
        if (size != 0) {
            for (int i = 0; i < size; i++) {
                String name = IOUtils.readRepetitiveString(in);
                Object value = IOUtils.readObject(in);
                _map.put(name, value);
            }
        }
    }

    private static class SerializationProcedure implements MapProcedure<String, Object> {
        private final ObjectOutput _out;
        private IOException _ioException;

        private SerializationProcedure(ObjectOutput out) {
            this._out = out;
        }

        public static boolean execute(Map<String, Object> map, ObjectOutput out) throws IOException {
            SerializationProcedure procedure = new SerializationProcedure(out);
            boolean result = CollectionsFactory.getInstance().forEachEntry(map, procedure);
            //boolean result = map.forEachEntry(procedure);
            if (procedure._ioException != null)
                throw procedure._ioException;

            return result;
        }

        @Override
        public boolean execute(String key, Object value) {
            try {
                IOUtils.writeRepetitiveString(_out, key);
                IOUtils.writeObject(_out, value);
                return true;
            } catch (IOException e) {
                _ioException = e;
                return false;
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return "DocumentProperties " + _map;
    }

    /**
     * Compares the specified object with this map for equality.  Returns <tt>true</tt> if the given
     * object is also a map and the two Maps represent the same mappings.  More formally, two maps
     * <tt>t1</tt> and <tt>t2</tt> represent the same mappings if <tt>t1.entrySet().equals(t2.entrySet())</tt>.
     * This ensures that the <tt>equals</tt> method works properly across different implementations
     * of the <tt>Map</tt> interface.
     *
     * @param obj object to be compared for equality with this map.
     * @return <tt>true</tt> if the specified object is equal to this map.
     */
    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;

        return _map.equals(obj);
    }

    /**
     * Returns the hash code value for this map.  The hash code of a map is defined to be the sum of
     * the hashCodes of each entry in the map's entrySet view.  This ensures that
     * <tt>t1.equals(t2)</tt> implies that <tt>t1.hashCode()==t2.hashCode()</tt> for any two maps
     * <tt>t1</tt> and <tt>t2</tt>, as required by the general contract of Object.hashCode.
     *
     * @return the hash code value for this map.
     * @see Object#hashCode()
     * @see Object#equals(Object)
     * @see #equals(Object)
     */
    @Override
    public int hashCode() {
        return _map.hashCode();
    }
}
