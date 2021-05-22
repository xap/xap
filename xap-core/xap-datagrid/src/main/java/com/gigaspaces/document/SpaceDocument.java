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

import com.gigaspaces.entry.VirtualEntry;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.utils.ObjectUtils;
import com.gigaspaces.serialization.SmartExternalizable;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Map;

/**
 * Represents a document which can be stored and retrieved from the space.
 *
 * A document is a collection of properties (which are name-value pairs) associated with a type. A
 * property name is a string. A property value can be of any type, but it is usually either a scalar
 * (e.g. int, String, Date, etc.), a set of properties (either {@link java.util.Map} or {@link
 * com.gigaspaces.document.DocumentProperties}), or an array/collection or scalars or maps.
 *
 * Documents of the same type can have different properties, which provides more dynamic schema than
 * POJOs.
 *
 * @author Niv Ingberg
 * @see com.gigaspaces.document.DocumentProperties
 * @see com.gigaspaces.metadata.SpaceTypeDescriptor
 * @since 8.0
 */

public class SpaceDocument implements VirtualEntry, SmartExternalizable {
    private static final long serialVersionUID = 1L;
    private static final byte _classVersion = 1;
    private static final String _defaultTypeName = Object.class.getName();

    private String _typeName;
    private int _version;
    private boolean _transient;
    private DocumentProperties _properties;

    private transient Map<String, Object> _propertiesView;

    /**
     * Creates a space document of type java.lang.Object.
     */
    public SpaceDocument() {
        this(_defaultTypeName, null);
    }

    /**
     * Creates a space document of the specified type.
     *
     * @param typeName Name of document's type.
     */
    public SpaceDocument(String typeName) {
        this(typeName, null);
    }

    /**
     * Creates a space document of the specified type with the specified properties.
     *
     * @param typeName   Name of document's type.
     * @param properties Properties to set in document.
     */
    public SpaceDocument(String typeName, Map<String, Object> properties) {
        setTypeName(typeName);
        this._properties = new DocumentProperties(properties);
        initialize();
    }

    protected void initialize() {
        this._propertiesView = Collections.unmodifiableMap(_properties);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getTypeName() {
        return _typeName;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SpaceDocument setTypeName(String typeName) {
        if (typeName == null || typeName.length() == 0)
            throw new IllegalArgumentException("Argument cannot be null or empty - 'typeName'.");
        this._typeName = typeName;
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean containsProperty(String name) {
        return _properties.containsKey(name);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> T getProperty(String name) {
        return (T) _properties.get(name);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SpaceDocument setProperty(String name, Object value) {
        _properties.put(name, value);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> T removeProperty(String name) {
        return (T) _properties.remove(name);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, Object> getProperties() {
        return _propertiesView;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SpaceDocument addProperties(Map<String, Object> properties) {
        if (properties != null)
            this._properties.putAll(properties);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getVersion() {
        return this._version;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SpaceDocument setVersion(int version) {
        this._version = version;
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isTransient() {
        return this._transient;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SpaceDocument setTransient(boolean isTransient) {
        this._transient = isTransient;
        return this;
    }

    @Override
    public String toString() {
        return "SpaceDocument [typeName=" + _typeName +
                ", version=" + _version +
                ", transient=" + _transient +
                ", properties=" + _properties + "]";
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (obj == null)
            return false;
        if (!(obj instanceof SpaceDocument))
            return false;
        SpaceDocument other = (SpaceDocument) obj;
        if (!(ObjectUtils.equals(this._typeName, other._typeName)))
            return false;
        if (this._transient != other._transient)
            return false;
        if (this._version != other._version)
            return false;
        if (!(ObjectUtils.equals(this._properties, other._properties)))
            return false;

        return true;
    }

//    public boolean cyclicReferenceSupportedEqual(Object obj, VerifiedCyclicReferenceContext verifiedEqualContext)
//    {
//    	if (obj == this)
//    		return true;
//    	if (obj == null)
//    		return false;
//    	if (!(obj instanceof SpaceDocument))
//    		return false;
//    	SpaceDocument other = (SpaceDocument) obj;
//    	if (!(ObjectUtils.equals(this._typeName, other._typeName)))
//    		return false;
//    	if (this._transient != other._transient)
//    		return false;
//    	if (this._version != other._version)
//    		return false;
//    	if (!(ObjectUtils.cyclicReferenceSupportedEqual(this._properties, other._properties, verifiedEqualContext)))
//    		return false;
//    	
//    	return true;
//    }

    @Override
    public int hashCode() {
        return _typeName.hashCode() * 31 + _properties.hashCode();
    }

    /**
     * This method is required by the {@link Externalizable} interface and should not be called
     * directly.
     */
    @Override
    public void writeExternal(ObjectOutput out)
            throws IOException {
        // Get target class version:
        final byte classVersion = _classVersion;
        // Write class version:
        out.writeByte(classVersion);
        // Write rest of data according to version:
        if (classVersion == 1)
            writeExternalV1(out);
        else
            throw new IOException("Failed to serialize SpaceDocument: unsupported class version  " + classVersion + ".");
    }

    private void writeExternalV1(ObjectOutput out)
            throws IOException {
        IOUtils.writeRepetitiveString(out, _typeName);
        out.writeBoolean(_transient);
        out.writeInt(_version);
        IOUtils.writeObject(out, _properties);
    }

    /**
     * This method is required by the {@link Externalizable} interface and should not be called
     * directly.
     */
    @Override
    public void readExternal(ObjectInput in)
            throws IOException, ClassNotFoundException {
        // Read class version:
        final byte classVersion = in.readByte();
        // Read rest of data according to version:
        if (classVersion == 1)
            readExternalV1(in);
        else
            throw new IOException("Failed to deserialize SpaceDocument: unsupported class version  " + classVersion + ".");
    }

    public void readExternalV1(ObjectInput in)
            throws IOException, ClassNotFoundException {
        this._typeName = IOUtils.readRepetitiveString(in);
        this._transient = in.readBoolean();
        this._version = in.readInt();
        this._properties = IOUtils.readObject(in);
        initialize();
    }

    public void initFromDocument(SpaceDocument document) {
        this.setTypeName(document._typeName);
        this.setTransient(document._transient);
        this.setVersion(document._version);
        this.addProperties(document._properties);
        initialize();
    }
}
