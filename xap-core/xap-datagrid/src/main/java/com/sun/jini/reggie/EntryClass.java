/*
 * 
 * Copyright 2005 Sun Microsystems, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * 	http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 */
package com.sun.jini.reggie;

import com.sun.jini.proxy.MarshalledWrapper;

import net.jini.io.OptimizedByteArrayOutputStream;
import net.jini.loader.ClassLoading;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.rmi.MarshalException;
import java.rmi.UnmarshalException;
import java.security.DigestOutputStream;
import java.security.MessageDigest;

/**
 * An EntryClass is a descriptor for an entry class, packaged up for transmission between
 * client-side proxies and the registrar server. Instances are never visible to clients, they are
 * private to the communication between the proxies and the server.  Note that we don't transmit
 * information about interfaces implemented by the class, because it isn't necessary given the
 * specific use of type information for entries. <p> This class only has a bare minimum of methods,
 * to minimize the amount of code downloaded into clients. <p>
 *
 * @author Sun Microsystems, Inc.
 * @see ClassMapper
 */
@com.gigaspaces.api.InternalApi
public class EntryClass implements Serializable {

    private static final long serialVersionUID = 2L;

    /**
     * Class name
     *
     * @serial
     */
    protected String name;
    /**
     * Hash for the type
     *
     * @serial
     */
    protected long hash;
    /**
     * Descriptor for the superclass
     *
     * @serial
     */
    protected EntryClass superclass;
    /**
     * Number of public fields
     *
     * @serial
     */
    protected int numFields;
    /**
     * Number of instances of this class in service registrations
     */
    protected transient int numInstances;
    /**
     * Number of templates of this class in event registrations
     */
    protected transient int numTemplates;
    /**
     * An instance containing only name and hash, no superclass info. This is only used on the
     * registrar side, to minimize the amount of info transmitted back to clients.
     */
    protected transient EntryClass replacement;
    /**
     * Flag set to true if this instance was unmarshalled from an integrity-protected stream, or
     * false otherwise
     */
    private transient boolean integrity = false;

    /**
     * Should only be called by ClassMapper
     */
    public EntryClass(Class clazz, EntryClass superclass)
            throws MarshalException {
        name = clazz.getName();
        this.superclass = superclass;
        ClassMapper.EntryField[] fields = ClassMapper.getFields(clazz);
        numFields = fields.length;
        computeHash(fields);
    }

    /**
     * Constructor used for creating replacement instances, containing only name and hash.
     */
    private EntryClass(EntryClass orig) {
        name = orig.name;
        hash = orig.hash;
    }

    /**
     * Return the superclass descriptor
     */
    public EntryClass getSuperclass() {
        return superclass;
    }

    /**
     * Return the number of public fields (including superclasses)
     */
    public int getNumFields() {
        return numFields;
    }

    /**
     * Set the number of instances of this class
     */
    public void setNumInstances(int numInstances) {
        this.numInstances = numInstances;
    }

    /**
     * Set the number of templates of this class
     */
    public void setNumTemplates(int numTemplates) {
        this.numTemplates = numTemplates;
    }

    /**
     * Return the replacement, if any, containing only name and rep.
     */
    public synchronized EntryClass getReplacement() {
        if (replacement == null)
            replacement = new EntryClass(this);
        return replacement;
    }

    /**
     * This is really only needed in the registrar, but it's very convenient to have here.
     *
     * @see Class#isAssignableFrom
     */
    public boolean isAssignableFrom(EntryClass cls) {
        for (EntryClass sup = cls; sup != null; sup = sup.superclass) {
            if (hash == sup.hash)
                return true;
        }
        return false;
    }

    /**
     * Returns the number of times this type is used in service registrations
     *
     * @return number of instances of this type in use in service registrations
     */
    public int getNumInstances() {
        return numInstances;
    }

    /**
     * Returns the number of times this type is used in event registrations
     *
     * @return number of times this type is used in event registrations
     */
    public int getNumTemplates() {
        return numTemplates;
    }

    // Converts this type descriptor to a Class object
    public Class toClass(String codebase)
            throws IOException, ClassNotFoundException {
        Class cls =
                ClassLoading.loadClass(codebase, name, null, integrity, null);
        EntryClass local;
        try {
            local = ClassMapper.toEntryClassBase(cls).eclass;
        } catch (MarshalException e) {
            throw new UnmarshalException("problem obtaining local version of "
                    + toString(), e);
        }
        if (hash != local.hash)
            throw new UnmarshalException("incoming entry type: " + toString()
                    + " is not assignable to the local"
                    + " version of the type: " + local);
        return cls;
    }

    /**
     * Returns the name of this type
     *
     * @return the name of this type
     */
    public String getName() {
        return name;
    }

    /**
     * Returns true if the object passed in is an instance of EntryClass with the same type hash as
     * this object.  Returns false otherwise.
     *
     * @param o object to compare this object against
     * @return true if this object equals the object passed in; false otherwise.
     */
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof EntryClass))
            return false;
        EntryClass t = (EntryClass) o;
        return hash == t.hash;
    }

    /**
     * Return a hashcode for this type.
     *
     * @return int the hashcode for this type
     */
    public int hashCode() {
        return (int) (hash ^ (hash >>> 32));
    }

    /* Inherit javadoc */
    public String toString() {
        return getClass() + "[name=" + getName() + ", hash=" + hash + "]";
    }

    /**
     * Computes a SHA-1 digest from the hash of the superclass, if there is a superclass, followed
     * by the name of this class, followed by the name and type for each field, if any, declared by
     * this class and ordered alphabetically by field name.  The first 8 bytes of the digest are
     * used to form the 64-bit hash value for this type.
     */
    private void computeHash(ClassMapper.EntryField[] fields)
            throws MarshalException {
        hash = 0;
        try {
            MessageDigest md = MessageDigest.getInstance("SHA");
            try (DataOutputStream out = new DataOutputStream(
                    new DigestOutputStream(new OptimizedByteArrayOutputStream(127), md))) {
                if (superclass != null)
                    out.writeLong(superclass.hash);
                out.writeUTF(name);
                int startDeclaredFields = superclass != null ?
                        superclass.numFields : 0;
                for (int i = startDeclaredFields; i < fields.length; i++) {
                    out.writeUTF(fields[i].field.getName());
                    out.writeUTF(fields[i].field.getType().getName());
                }
                out.flush();
            }
            byte[] digest = md.digest();
            for (int i = Math.min(8, digest.length); --i >= 0; ) {
                hash += ((long) (digest[i] & 0xFF)) << (i * 8);
            }
        } catch (Exception e) {
            throw new MarshalException("Unable to calculate type hash for "
                    + name, e);
        }
    }

    /**
     * Samples integrity protection setting (if any) of the stream from which this instance is being
     * deserialized and checks that valid values for this object have been read from the stream.
     */
    private void readObject(ObjectInputStream in)
            throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        if (name == null)
            throw new InvalidObjectException("name cannot be null");
        if (hash == 0)
            throw new InvalidObjectException("hash cannot be zero");
        integrity = MarshalledWrapper.integrityEnforced(in);
    }

    /**
     * Throws InvalidObjectException, since data for this class is required.
     */
    private void readObjectNoData() throws InvalidObjectException {
        throw new InvalidObjectException("no data");
    }

}
