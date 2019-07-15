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
package net.jini.io;

import net.jini.io.context.IntegrityEnforcement;

import java.io.IOException;
import java.io.InputStream;
import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamClass;
import java.io.ObjectStreamException;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/*
 * Implementation note: This class uses the helper class
 * MarshalledObject that is in this package. To avoid confusion
 * with java.rmi.MarshalledObject the fully qualified class names
 * are used for both classes.
 */

/**
 * A <code>MarshalledInstance</code> contains an object in serialized form. The contained object can
 * be deserialized on demand when explicitly requested. This allows an object to be sent from one VM
 * to another in a way that allows the receiver to control when and if the object is deserialized.
 * <p> The contained object is specified at construction time and can either be provided in
 * unserialized or serialized form. If provided in unserialized form it will be serialized during
 * construction with the serialization semantics defined by <code>MarshalOutputStream</code>. In
 * particular, classes are annotated with a codebase URL from which the class can be loaded (if
 * available). <p> If the <code>MarshalledInstance</code> needs to deserialize the contained object
 * then the contained object will be deserialized with the deserialization semantics defined by
 * <code>MarshalInputStream</code>. In particular, the codebase annotations associated with the
 * contained object may be used to load classes referenced by the contained object. <p>
 * <code>MarshalledInstance</code> provides functionality similar to
 * <code>java.rmi.MarshalledObject</code>, but additionally provides for the verification of
 * codebase integrity. Unlike <code>java.rmi.MarshalledObject</code>, it does not perform remote
 * object-to-stub replacement.
 *
 * @author Sun Microsystems, Inc.
 * @since 2.0
 */
@com.gigaspaces.api.InternalApi
public class MarshalledInstance implements Serializable {

    public static final String USE_GZIP_PROPERTY = "com.gs.lookup.useGzip";

    private static final boolean USE_GZIP = System.getProperty(USE_GZIP_PROPERTY, "false").equalsIgnoreCase("true");

    private interface HeaderBitMap {
        // denotes that GZIP is used in the header
        byte GZIP = 1 << 0;
    }

    /**
     * @serial Bytes of serialized representation.  If <code>objBytes</code> is <code>null</code>
     * then the object marshalled was a <code>null</code> reference.
     */
    private byte[] objBytes = null;

    /**
     * @serial Bytes of location annotations, which are ignored by <code>equals</code>.  If
     * <code>locBytes</code> is null, there were no non-<code>null</code> annotations during
     * marshalling.
     */
    private byte[] locBytes = null;

    /**
     * @serial Stored hash code of contained object.
     * @see #hashCode
     */
    private int hash;

    static final long serialVersionUID = -5187033771082433496L;

    /**
     * Creates a new <code>MarshalledInstance</code> that contains the marshalled representation of
     * the current state of the supplied object. The object is serialized with the semantics defined
     * by <code>MarshalOutputStream</code>. The output stream used to marshal the object implements
     * {@link ObjectStreamContext} and returns an empty collection from its {@link
     * ObjectStreamContext#getObjectStreamContext getObjectStreamContext} method.
     *
     * @param obj The Object to be contained in the new <code>MarshalledInstance</code>
     * @throws IOException if the object cannot be serialized
     */
    public MarshalledInstance(Object obj) throws IOException {
        this(obj, Collections.EMPTY_SET);
    }

    /**
     * Creates a new <code>MarshalledInstance</code> that contains the marshalled representation of
     * the current state of the supplied object. The object is serialized with the semantics defined
     * by <code>MarshalOutputStream</code>. The output stream used to marshal the object implements
     * {@link ObjectStreamContext} and returns the given collection from its {@link
     * ObjectStreamContext#getObjectStreamContext getObjectStreamContext} method.
     *
     * @param obj     The Object to be contained in the new <code>MarshalledInstance</code>
     * @param context the collection of context information objects
     * @throws IOException          if the object cannot be serialized
     * @throws NullPointerException if <code>context</code> is <code>null</code>
     */
    public MarshalledInstance(Object obj, Collection context)
            throws IOException {
        if (context == null)
            throw new NullPointerException();

        if (obj == null) {
            hash = 13;        // null hash for java.rmi.MarshalledObject
            return;
        }
        OptimizedByteArrayOutputStream bByteOut = new OptimizedByteArrayOutputStream();
        bByteOut.writeByte(buildFlag());
        OptimizedByteArrayOutputStream lByteOut = new OptimizedByteArrayOutputStream();
        lByteOut.writeByte(buildFlag());
        try (OutputStream bout = USE_GZIP ? new GZIPOutputStream(bByteOut) : bByteOut) {
            try (OutputStream lout = USE_GZIP ? new GZIPOutputStream(lByteOut) : lByteOut) {
                try (MarshalledInstanceOutputStream out = new MarshalledInstanceOutputStream(bout, lout, context)) {
                    out.writeObject(obj);
                    out.flush();
                    bout.close();
                    lout.close();
                    objBytes = bByteOut.toByteArray();
                    // locBytes is null if no annotations
                    locBytes = (out.hadAnnotations() ? lByteOut.toByteArray() : null);
                }
            }
        }

        // Calculate hash from the marshalled representation of object
        // so the hashcode will be comparable when sent between VMs.
        //
        // Note: This calculation must match the calculation in
        //	 java.rmi.MarshalledObject since we use this hash
        //	 in the converted MarshalledObject. The reverse is
        //	 also true in that we use the MarshalledObject's
        //	 hash for our hash. (see the MarshalledInstance(
        //	 MarshalledObject) constructor)
        //
        int h = 0;
        for (int i = 0; i < objBytes.length; i++) {
            h = 31 * h + objBytes[i];
        }
        hash = h;
    }

    /**
     * Creates a new <code>MarshalledInstance</code> from an existing <code>MarshalledObject</code>.
     * An object equivalent to the object contained in the passed <code>MarshalledObject</code> will
     * be contained in the new <code>MarshalledInstance</code>. <p> The object contained in the
     * passed <code>MarshalledObject</code> will not be unmarshalled as part of this call.
     *
     * @param mo The <code>MarshalledObject</code> that contains the object the new
     *           <code>MarshalledInstance</code> should contain
     * @throws NullPointerException if <code>mo</code> is <code>null</code>
     */
    public MarshalledInstance(java.rmi.MarshalledObject mo) {

        if (mo == null)
            throw new NullPointerException();

        // To extract the java.rmi.MarshalledObject's fields we
        // convert the mo into a net.jini.io.MarshalledObject.
        // (See resolveClass() in FromMOInputStream) The private
        // version of MarshalledObject allows access to the needed
        // fields.
        //
        net.jini.io.MarshalledObject privateMO = null;
        try {
            OptimizedByteArrayOutputStream baos = new OptimizedByteArrayOutputStream();
            baos.writeByte(buildFlag());
            try (OutputStream os = USE_GZIP ? new GZIPOutputStream(baos) : baos) {
                try (ObjectOutputStream oos = new ObjectOutputStream(os)) {
                    oos.writeObject(mo);
                    oos.flush();
                }
            }

            byte[] bytes = baos.toByteArray();
            OptimizedByteArrayInputStream bais = new OptimizedByteArrayInputStream(bytes);
            byte flags = bais.readByte();
            try (InputStream is = isGzip(flags) ? new GZIPInputStream(bais) : bais) {
                try (ObjectInputStream ois = new FromMOInputStream(is)) {
                    privateMO =
                            (MarshalledObject) ois.readObject();
                }
            }
        } catch (IOException | ClassNotFoundException ioe) {
            throw new AssertionError(ioe);
        }
        objBytes = privateMO.objBytes;
        locBytes = privateMO.locBytes;
        hash = privateMO.hash;
    }

    /**
     * Creates a new <code>MarshalledObject</code> that will contain an object equivalent to the
     * object contained in this <code>MarshalledInstance</code> object. <p> The object contained in
     * this <code>MarshalledInstance</code> object will not be unmarshalled as part of this call.
     *
     * @return A new <code>MarshalledObject</code> which contains an object equivalent to the object
     * contained in this <code>MarshalledInstance</code>
     */
    public java.rmi.MarshalledObject convertToMarshalledObject() {

        // To create a java.rmi.MarshalledObject with previously
        // serialized data we first create a private
        // net.jini.io.MarshalledObject with the
        // data and then convert it to the final object by changing
        // the class during readObject(). (See resolveClass() in
        // ToMOInputStream)
        //
        net.jini.io.MarshalledObject privateMO =
                new net.jini.io.MarshalledObject();

        privateMO.objBytes = objBytes;
        privateMO.locBytes = locBytes;
        privateMO.hash = hash;

        java.rmi.MarshalledObject mo = null;
        try {
            byte[] bytes;
            try (OptimizedByteArrayOutputStream baos = new OptimizedByteArrayOutputStream()) {
                baos.writeByte(buildFlag());
                try (OutputStream os = USE_GZIP ? new GZIPOutputStream(baos) : baos) {
                    try (ObjectOutputStream oos = new ObjectOutputStream(os)) {
                        oos.writeObject(privateMO);
                        oos.flush();
                    }
                }
                bytes = baos.toByteArray();
            }
            OptimizedByteArrayInputStream bais = new OptimizedByteArrayInputStream(bytes);
            byte flags = bais.readByte();
            try (InputStream is = isGzip(flags) ? new GZIPInputStream(bais) : bais) {
                try (ObjectInputStream ois = new ToMOInputStream(is)) {
                    mo = (java.rmi.MarshalledObject) ois.readObject();
                }
            }
        } catch (IOException | ClassNotFoundException ioe) {
            throw new AssertionError(ioe);
        }
        return mo;
    }

    /**
     * Returns a new copy of the contained object. Deserialization is performed with the semantics
     * defined by <code>MarshalInputStream</code>. The input stream used to unmarshal the object
     * implements {@link ObjectStreamContext} and returns a collection from its {@link
     * ObjectStreamContext#getObjectStreamContext getObjectStreamContext} method which contains a
     * single element of type {@link IntegrityEnforcement}; the {@link
     * IntegrityEnforcement#integrityEnforced integrityEnforced} method of this element returns the
     * specified <code>verifyCodebaseIntegrity</code> value. <p>MarshalledInstance</code> implements
     * this method by calling <code>{@link #get(ClassLoader, boolean, ClassLoader, Collection)
     * get}(null, verifyCodebaseIntegrity, null, null)</code>.
     *
     * @param verifyCodebaseIntegrity if <code>true</code> then codebase integrity is verified,
     *                                otherwise code base integrity is not verified
     * @return a new copy of the contained object
     * @throws IOException            if an <code>IOException</code> occurs while deserializing the
     *                                object from its internal representation
     * @throws ClassNotFoundException if any classes necessary for reconstructing the contained
     *                                object can not be found or if <code>verifyCodebaseIntegrity</code>
     *                                is <code>true</code> and the integrity of the contained
     *                                object's codebase cannot be confirmed
     */
    public Object get(final boolean verifyCodebaseIntegrity)
            throws IOException, ClassNotFoundException {
        return get(null, verifyCodebaseIntegrity, null, null);
    }

    /**
     * Returns a new copy of the contained object. Deserialization is performed with the semantics
     * defined by <code>MarshalInputStream</code>. If <code>context</code> is not <code>null</code>
     * the input stream used to unmarshal the object implements {@link ObjectStreamContext} and
     * returns the given collection from its {@link ObjectStreamContext#getObjectStreamContext
     * getObjectStreamContext} method. <p>If <code>context</code> is <code>null</code> the input
     * stream used to unmarshal the object implements {@link ObjectStreamContext} and returns a
     * collection from its {@link ObjectStreamContext#getObjectStreamContext getObjectStreamContext}
     * method which contains a single element of type {@link IntegrityEnforcement}; the {@link
     * IntegrityEnforcement#integrityEnforced integrityEnforced} method of this element returns the
     * specified <code>verifyCodebaseIntegrity</code> value.
     *
     * @param defaultLoader           the class loader value (possibly <code>null</code>) to pass as
     *                                the <code>defaultLoader</code> argument to
     *                                <code>RMIClassLoader</code> methods
     * @param verifyCodebaseIntegrity if <code>true</code> then codebase integrity is verified,
     *                                otherwise code base integrity is not verified
     * @param verifierLoader          the class loader value (possibly <code>null</code>) to pass to
     *                                {@link net.jini.security.Security#verifyCodebaseIntegrity
     *                                Security.verifyCodebaseIntegrity}, if <code>verifyCodebaseIntegrity</code>
     *                                is <code>true</code>
     * @param context                 the collection of context information objects or
     *                                <code>null</code>
     * @return a new copy of the contained object
     * @throws IOException            if an <code>IOException</code> occurs while deserializing the
     *                                object from its internal representation
     * @throws ClassNotFoundException if any classes necessary for reconstructing the contained
     *                                object can not be found or if <code>verifyCodebaseIntegrity</code>
     *                                is <code>true</code> and the integrity of the contained
     *                                object's codebase cannot be confirmed
     */
    public Object get(ClassLoader defaultLoader,
                      final boolean verifyCodebaseIntegrity,
                      ClassLoader verifierLoader,
                      Collection context)
            throws IOException, ClassNotFoundException {
        if (objBytes == null)   // must have been a null object
            return null;

        if (context == null) {
            context = Collections.singleton(
                    new IntegrityEnforcement() {
                        public boolean integrityEnforced() {
                            return verifyCodebaseIntegrity;
                        }
                    });
        }
        OptimizedByteArrayInputStream bin = new OptimizedByteArrayInputStream(objBytes);
        byte bflags = bin.readByte();
        InputStream lis = null;
        Object obj;
        try (InputStream bis = isGzip(bflags) ? new GZIPInputStream(bin) : bin) {
            // locBytes is null if no annotations
            OptimizedByteArrayInputStream lin =
                    (locBytes == null ? null : new OptimizedByteArrayInputStream(locBytes));
            lis = lin;
            if (lin != null) {
                byte flags = lin.readByte();
                if (isGzip(flags)) {
                    lis = new GZIPInputStream(lis);
                }
            }
            try (MarshalledInstanceInputStream in = new MarshalledInstanceInputStream(bis, lis,
                    defaultLoader,
                    verifyCodebaseIntegrity,
                    verifierLoader,
                    context)) {
                in.useCodebaseAnnotations();
                obj = in.readObject();
            }
        } finally {
            if (lis != null) {
                lis.close();
            }
        }

        return obj;
    }

    /**
     * Compares this <code>MarshalledInstance</code> to another object. Returns true if and only if
     * the argument refers to an instance of <code>MarshalledInstance</code> that contains exactly
     * the same serialized form for its contained object as this object does and has the same class
     * codebase annotations.
     *
     * @param obj the object to compare with this <code>MarshalledInstance</code>
     * @return <code>true</code> if the argument contains an object with an equivalent serialized
     * form and codebase; otherwise returns <code>false</code>
     */
    public boolean fullyEquals(Object obj) {
        if (equals(obj)) {
            MarshalledInstance other = (MarshalledInstance) obj;
            return Arrays.equals(locBytes, other.locBytes);
        }
        return false;
    }

    /**
     * Compares this <code>MarshalledInstance</code> to another object. Returns true if and only if
     * the argument refers to an instance of <code>MarshalledInstance</code> that contains exactly
     * the same serialized form for its contained object as this object does. The comparison ignores
     * any class codebase annotations, so that two objects can be equivalent if they have the same
     * serialized representation, except for the codebase of each class in the serialized
     * representation.
     *
     * @param obj the object to compare with this <code>MarshalledInstance</code>
     * @return <code>true</code> if the argument contains an object with an equivalent serialized
     * form; otherwise returns <code>false</code>
     */
    public boolean equals(Object obj) {
        if (obj == this)
            return true;

        if (obj instanceof MarshalledInstance) {
            MarshalledInstance other = (MarshalledInstance) obj;
            if (hash != other.hash)
                return false;
            return Arrays.equals(objBytes, other.objBytes);
        }
        return false;
    }

    /**
     * Returns the hash code for this <code>MarshalledInstance</code>. The hash code is calculated
     * only from the serialized form of the contained object.
     *
     * @return The hash code for this object
     */
    public int hashCode() {
        return hash;
    }

    /**
     * Verify the case of null contained object.
     */
    private void readObject(ObjectInputStream in)
            throws IOException, ClassNotFoundException {
        in.defaultReadObject();

        // If contained object is null, then hash and locBytes must be
        // proper
        //
        if ((objBytes == null) && ((hash != 13) || (locBytes != null)))
            throw new InvalidObjectException("Bad hash or annotation");
    }

    /**
     * Protect against missing superclass.
     */
    private void readObjectNoData() throws ObjectStreamException {
        throw new InvalidObjectException("Bad class hierarchy");
    }

    /**
     * This class is used to marshal objects for <code>MarshalledInstance</code>.  It places the
     * location annotations to one side so that two <code>MarshalledInstance</code>s can be compared
     * for equality if they differ only in location annotations.  Objects written using this stream
     * should be read back from a <code>MarshalledInstanceInputStream</code>.
     *
     * @see MarshalledInstanceInputStream
     */
    private static class MarshalledInstanceOutputStream
            extends MarshalOutputStream {
        /**
         * The stream on which location objects are written.
         */
        private ObjectOutputStream locOut;

        /**
         * <code>true</code> if non-<code>null</code> annotations are written.
         */
        private boolean hadAnnotations;

        /**
         * Creates a new <code>MarshalledObjectOutputStream</code> whose non-location bytes will be
         * written to <code>objOut</code> and whose location annotations (if any) will be written to
         * <code>locOut</code>.
         */
        public MarshalledInstanceOutputStream(OutputStream objOut,
                                              OutputStream locOut,
                                              Collection context)
                throws IOException {
            super(objOut, context);
            this.locOut = new ObjectOutputStream(locOut);
            hadAnnotations = false;
        }

        /**
         * Returns <code>true</code> if any non-<code>null</code> location annotations have been
         * written to this stream.
         */
        public boolean hadAnnotations() {
            return hadAnnotations;
        }

        /**
         * Overrides <code>MarshalOutputStream.writeAnnotation</code> implementation to write
         * annotations to the location stream.
         */
        protected void writeAnnotation(String loc) throws IOException {
            hadAnnotations |= (loc != null);
            locOut.writeObject(loc);
        }

        public void flush() throws IOException {
            super.flush();
            locOut.flush();
        }
    }

    /**
     * The counterpart to <code>MarshalledInstanceOutputStream</code>.
     *
     * @see MarshalledInstanceOutputStream
     */
    private static class MarshalledInstanceInputStream
            extends MarshalInputStream {
        /**
         * The stream from which annotations will be read.  If this is <code>null</code>, then all
         * annotations were <code>null</code>.
         */
        private ObjectInputStream locIn;

        /**
         * Creates a new <code>MarshalledObjectInputStream</code> that reads its objects from
         * <code>objIn</code> and annotations from <code>locIn</code>.  If <code>locIn</code> is
         * <code>null</code>, then all annotations will be <code>null</code>.
         */
        MarshalledInstanceInputStream(InputStream objIn,
                                      InputStream locIn,
                                      ClassLoader defaultLoader,
                                      boolean verifyCodebaseIntegrity,
                                      ClassLoader verifierLoader,
                                      Collection context)
                throws IOException {
            super(objIn,
                    defaultLoader,
                    verifyCodebaseIntegrity,
                    verifierLoader,
                    context);
            this.locIn = (locIn == null ? null : new ObjectInputStream(locIn));
        }

        /**
         * Overrides <code>MarshalInputStream.readAnnotation</code> to return locations from the
         * stream we were given, or <code>null</code> if we were given a <code>null</code> location
         * stream.
         */
        protected String readAnnotation()
                throws IOException, ClassNotFoundException {
            return (locIn == null ? null : (String) locIn.readObject());
        }
    }

    /**
     * Input stream to convert <code>java.rmi.MarshalledObject</code> into
     * <code>net.jini.io.MarshalledObject</code>.
     */
    private static class FromMOInputStream extends ObjectInputStream {

        public FromMOInputStream(InputStream in) throws IOException {
            super(in);
        }

        /**
         * Overrides <code>ObjectInputStream.resolveClass</code> to change an occurence of class
         * <code>java.rmi.MarshalledObject</code> to class <code>net.jini.io.MarshalledObject</code>.
         */
        protected Class resolveClass(ObjectStreamClass desc)
                throws IOException, ClassNotFoundException {
            if (desc.getName().equals("java.rmi.MarshalledObject")) {
                return net.jini.io.MarshalledObject.class;
            }
            return super.resolveClass(desc);
        }
    }

    /**
     * Input stream to convert <code>net.jini.io.MarshalledObject</code> into
     * <code>java.rmi.MarshalledObject</code>.
     */
    private static class ToMOInputStream extends ObjectInputStream {

        public ToMOInputStream(InputStream in) throws IOException {
            super(in);
        }

        /**
         * Overrides <code>ObjectInputStream.resolveClass</code> to change an occurence of class
         * <code>net.jini.io.MarshalledObject</code> to class <code>java.rmi.MarshalledObject</code>.
         */
        protected Class resolveClass(ObjectStreamClass desc)
                throws IOException, ClassNotFoundException {
            if (desc.getName().equals("net.jini.io.MarshalledObject")) {
                return java.rmi.MarshalledObject.class;
            }
            return super.resolveClass(desc);
        }
    }

    private byte buildFlag() {
        byte flags = 0;
        if (USE_GZIP) {
            flags |= HeaderBitMap.GZIP;
        }
        return flags;
    }

    private boolean isGzip(byte flags) {
        return (flags & HeaderBitMap.GZIP) != 0;
    }
}
