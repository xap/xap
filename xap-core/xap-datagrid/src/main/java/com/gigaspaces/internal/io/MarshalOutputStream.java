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

package com.gigaspaces.internal.io;


import com.gigaspaces.internal.classloader.ClassLoaderCache;
import com.gigaspaces.internal.classloader.IClassLoaderCacheStateListener;
import com.gigaspaces.internal.collections.CollectionsFactory;
import com.gigaspaces.internal.collections.ObjectIntegerMap;
import com.gigaspaces.logger.Constants;
import com.gigaspaces.lrmi.LRMIInvocationContext;
import com.j_spaces.kernel.ClassLoaderHelper;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamClass;
import java.io.OutputStream;
import java.rmi.server.RMIClassLoader;
import java.util.HashMap;
import java.util.HashSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * An extension of <code>ObjectOutputStream</code> that implements the dynamic class loading
 * semantics of RMI argument and result marshalling (using {@link RMIClassLoader}).  A
 * <code>MarshalOutputStream</code> writes data that is intended to be written by a corresponding
 * {@link MarshalInputStream}.
 *
 * <p><code>MarshalOutputStream</code> implements the output side of the dynamic class loading
 * semantics by overriding {@link ObjectOutputStream#annotateClass annotateClass} and {@link
 * ObjectOutputStream#annotateProxyClass annotateProxyClass} to annotate class descriptors in the
 * stream with codebase strings obtained using {@link RMIClassLoader#getClassAnnotation
 * RMIClassLoader.getClassAnnotation}.
 *
 * <p><code>MarshalOutputStream</code> writes class annotations to its own stream; a subclass may
 * override the {@link #writeAnnotation writeAnnotation} method to write the class annotations to a
 * different location.
 *
 * <p><code>MarshalOutputStream</code> does not modify the stream protocol version of its instances'
 * superclass state (see {@link ObjectOutputStream#useProtocolVersion
 * ObjectOutputStream.useProtocolVersion}).
 *
 * @author Igor Goldenberg
 * @author anna
 * @since 5.0
 **/
@com.gigaspaces.api.InternalApi
public class MarshalOutputStream
        extends AnnotatedObjectOutputStream
        implements IMarshalOutputStream {
    private static final Logger _logger = LoggerFactory.getLogger(Constants.LOGGER_LRMI_MARSHAL);
    private static final int CODE_DISABLED = -1;
    private static final int CODE_NULL = 0;

    private final Context _context;
    private final boolean _optimize;
    /**
     * Next index to for the next ObjectStreamClass mapped. starts from 1, since 0 indicates "null"
     * in TObjectIntHashMap
     */
    private int _nextClassId = 1;

    public MarshalOutputStream(OutputStream out) throws IOException {
        this(out, true);
    }

    /**
     * Creates a new <code>MarshalOutputStream</code> that writes marshalled data to the specified
     * underlying <code>OutputStream</code>.
     *
     * <p>This constructor passes <code>out</code> to the superclass constructor that has an
     * <code>OutputStream</code> parameter.
     *
     * <p><code>context</code> will be used as the return value of the created stream's {@link
     * #getObjectStreamContext getObjectStreamContext} method.
     *
     * @param out      the output stream to write marshalled data to
     * @param context  the collection of context information objects to be returned by this stream's
     *                 {@link #getObjectStreamContext getObjectStreamContext} method
     * @param optimize whether to activate the class description optimization during serialization.
     * @throws IOException          if the superclass's constructor throws an <code>IOException</code>
     * @throws SecurityException    if the superclass's constructor throws a <code>SecurityException</code>
     * @throws NullPointerException if <code>out</code> or <code>context</code> is
     *                              <code>null</code>
     **/
    public MarshalOutputStream(OutputStream out, boolean optimize) throws IOException {
        this(out, optimize, new Context());
    }

    public MarshalOutputStream(OutputStream out, boolean optimize, Context context) throws IOException {
        super(out);
        _optimize = optimize;
        _context = context;
    }

    /**
     * Writes a repetitive object. If this is the first time this object is written, it is assigned
     * a code which is written along the object. The next time the object is written using this
     * method, only its code is written.
     *
     * NOTE: Objects written using this method are cached and are NEVER cleaned up. If the object
     * you are writing is using a PU resource it will not be released when the PU is terminated, and
     * will cause a leak.
     */
    @Override
    public void writeRepetitiveObject(Object obj)
            throws IOException {
        // If object is null, write null code:
        if (obj == null) {
            writeInt(CODE_NULL);
            return;
        }
        // If optimization is disabled, write code and object:
        if (!_optimize) {
            writeInt(CODE_DISABLED);
            writeObject(obj);
            return;
        }

        // Look for object in cache:
        int code = _context.getRepetitiveObjectsCache().get(obj);

        // If object is cached, write only its code:
        if (code != CODE_NULL) {
            writeInt(code);
            return;
        }

        // Create new code:
        code = _context.getAndIncrementRepetitiveObjectCounter();

        // Put in cache:
        _context.getRepetitiveObjectsCache().put(obj, code);
        // Write code and object:
        writeInt(code);
        writeObject(obj);
    }

    /**
     * First checks that this class wasn't written already, if it does do nothing.
     *
     * <p>Else invokes {@link RMIClassLoader#getClassAnnotation RMIClassLoader.getClassAnnotation}
     * with <code>cl</code> to get the appropriate class annotation string value (possibly
     * <code>null</code>) and then writes a class annotation string value (possibly
     * <code>null</code>) to be read by a corresponding <code>MarshalInputStream</code>
     * implementation.
     *
     * <p><code>MarshalOutputStream</code> implements this method to just write the annotation value
     * to this stream using {@link ObjectOutputStream#writeUnshared writeUnshared}.
     *
     * <p>A subclass can override this method to write the annotation to a different location.
     *
     * @param annotation the class annotation string value (possibly <code>null</code>) to write
     * @throws IOException if I/O exception occurs writing the annotation
     **/
    @Override
    protected void writeAnnotation(Class cl)
            throws IOException {
        if (_context.containsAnnotation(cl))
            return;

        _context.addAnnotation(cl);
        super.writeAnnotation(cl);
    }


    /**
     * Overrides the original implementation, tries to save sending class descriptor, first checks
     * if the class was already sent. If it does send only the class index otherwise send the class
     * descriptor also.
     *
     * @param desc class descriptor to write to the stream.
     * @throws IOException If an I/O error has occurred.
     */
    @Override
    protected void writeClassDescriptor(ObjectStreamClass desc) throws IOException {
        int i = _context.getObjectStreamClassKey(desc);
        boolean newClass = false;
        if (i == 0 || !_optimize) {
            _logger.info("new class: {} (size: {})", desc.getName(), _nextClassId);
            newClass = true;
            if (_optimize) {
                i = _nextClassId++;
                _context.putObjectStreamClassKey(desc, i);// classMap.put(desc, i);
            } else {
                i = -1;
            }
        }

        writeInt(i); // write class ID
        if (newClass) // new class
        {
            super.writeClassDescriptor(desc);
        }
    }

    @Override
    protected void writeStreamHeader() throws IOException {
        writeByte(TC_RESET);
    }

    public void closeContext() {
        _context.close();
    }

    public void resetContext() {
        _context.reset();
    }

    private static void logFinest(String log) {
        if (_logger.isTraceEnabled()) {
            _logger.trace(log + ", context=" + LRMIInvocationContext.getContextMethodLongDisplayString());
        }
    }

    /**
     * @author Barak
     * @since 6.6
     */
    public static class Context implements IClassLoaderCacheStateListener {

        private final HashMap<Long, ClassLoaderContext> _classLoaderContextMap = new HashMap<Long, ClassLoaderContext>();
        private final ObjectIntegerMap<Object> _repetitiveObjectsCache = CollectionsFactory.getInstance().createObjectIntegerMap();
        private int _repetitiveObjectCounter = CODE_NULL + 1;

        private final static ClassLoaderContext REMOVED_CONTEXT_MARKER = new ClassLoaderContext(-2L);
        private final static long NULL_CL_MARKER = -1;

        public synchronized void putObjectStreamClassKey(ObjectStreamClass desc, int key) {
            ClassLoaderContext classLoaderContext = getClassLoaderContext(true);
            classLoaderContext.putObjectStreamClass(desc, key);
            if (_logger.isTraceEnabled())
                logFinest("Adding new ObjectStreamClass to outgoing context [" + desc.getName() + "] with specified key " + key + ", context class loader key " + classLoaderContext.getClassLoaderCacheKey());
        }

        public synchronized void addAnnotation(Class<?> cl) {
            ClassLoaderContext classLoaderContext = getClassLoaderContext(true);
            classLoaderContext.addAnnotation(cl);
            if (_logger.isTraceEnabled())
                logFinest("Adding new annotation to outgoing context [" + cl.getName() + "], context class loader key " + classLoaderContext.getClassLoaderCacheKey());
        }

        public synchronized boolean containsAnnotation(Class<?> cl) {
            ClassLoaderContext classLoaderContext = getClassLoaderContext(false);
            if (classLoaderContext == null)
                return false;
            return classLoaderContext.containsAnnotation(cl);
        }

        public synchronized int getObjectStreamClassKey(ObjectStreamClass desc) {
            ClassLoaderContext classLoaderContext = getClassLoaderContext(false);
            if (classLoaderContext == null)
                return 0;
            return classLoaderContext.getObjectStreamClassKey(desc);
        }

        private ClassLoaderContext getClassLoaderContext(boolean createIfAbsent) {
            ClassLoader contextClassLoader = ClassLoaderHelper.getContextClassLoader();
            Long clKey = contextClassLoader == null ? NULL_CL_MARKER : ClassLoaderCache.getCache().putClassLoader(contextClassLoader);
            ClassLoaderContext classLoaderContext = _classLoaderContextMap.get(clKey);
            if (classLoaderContext == REMOVED_CONTEXT_MARKER)
                throw new MarshalContextClearedException("Service has been unloaded");
            if (createIfAbsent && classLoaderContext == null) {
                classLoaderContext = new ClassLoaderContext(clKey);
                _classLoaderContextMap.put(clKey, classLoaderContext);
                if (clKey != NULL_CL_MARKER) {
                    boolean classLoaderPresent = ClassLoaderCache.getCache().registerClassLoaderStateListener(clKey, this);
                    //Double check, class loader could have been removed from last check till now
                    if (!classLoaderPresent) {
                        _classLoaderContextMap.remove(clKey);
                        throw new MarshalContextClearedException("Service has been unloaded");
                    }
                }

            }
            return classLoaderContext;
        }

        public synchronized void onClassLoaderRemoved(Long classLoaderKey, boolean explicit) {
            if (_logger.isTraceEnabled())
                logFinest("Removed class loader [" + classLoaderKey + "] related context from marshal output stream, explicit=" + explicit);
            _classLoaderContextMap.put(classLoaderKey, REMOVED_CONTEXT_MARKER);
        }

        public ObjectIntegerMap<Object> getRepetitiveObjectsCache() {
            return _repetitiveObjectsCache;
        }

        public int getAndIncrementRepetitiveObjectCounter() {
            return _repetitiveObjectCounter++;
        }

        public synchronized void close() {
            if (_logger.isTraceEnabled())
                logFinest("Closing marshal output stream context");
            for (Long clKey : _classLoaderContextMap.keySet()) {
                _classLoaderContextMap.put(clKey, REMOVED_CONTEXT_MARKER);
                ClassLoaderCache.getCache().removeClassLoaderStateListener(clKey, this);
            }
        }

        public synchronized void reset() {
            if (_logger.isTraceEnabled())
                logFinest("Resetting entire marshal output stream context");
            for (Long clKey : _classLoaderContextMap.keySet()) {
                _classLoaderContextMap.put(clKey, REMOVED_CONTEXT_MARKER);
                ClassLoaderCache.getCache().removeClassLoaderStateListener(clKey, this);
            }
            _classLoaderContextMap.clear();
            _repetitiveObjectsCache.clear();
            _repetitiveObjectCounter = CODE_NULL + 1;
        }

        /**
         * Context per class loader
         *
         * @author eitany
         * @since 7.0.1
         */
        private static class ClassLoaderContext {
            /**
             * Maps ObjectStreamClass to an index in order to save resending of ObjectStreamClass.
             */
            private final ObjectIntegerMap<ObjectStreamClass> classMap = CollectionsFactory.getInstance().createObjectIntegerMap();
            /**
             * Save classes that their annotation was already sent.
             */
            private final HashSet<Class> annotateMap = new HashSet<Class>();
            private final Long clKey;

            public ClassLoaderContext(Long clKey) {
                this.clKey = clKey;
            }

            public void putObjectStreamClass(ObjectStreamClass oss, int key) {
                classMap.put(oss, key);
            }

            public Long getClassLoaderCacheKey() {
                return clKey;
            }

            public int getObjectStreamClassKey(ObjectStreamClass desc) {
                return classMap.get(desc);
            }

            public boolean containsAnnotation(Class<?> cl) {
                return annotateMap.contains(cl);
            }

            public void addAnnotation(Class<?> cl) {
                annotateMap.add(cl);
            }
        }

    }
}
