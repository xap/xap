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
import com.gigaspaces.internal.collections.IntegerObjectMap;
import com.gigaspaces.internal.reflection.IConstructor;
import com.gigaspaces.internal.reflection.ReflectionUtil;
import com.gigaspaces.logger.Constants;
import com.gigaspaces.lrmi.LRMIInvocationContext;
import com.j_spaces.kernel.ClassLoaderHelper;
import com.j_spaces.kernel.ISafeArray;
import com.j_spaces.kernel.SafeArray;

import java.io.Externalizable;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectStreamClass;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An extension of <code>AnnotatedObjectInputStream</code> that provides local storage of already
 * resolved class descriptors, to optimize the marshaling.
 *
 * @author Igor Goldenberg
 * @author anna
 * @since 5.0
 **/
@com.gigaspaces.api.InternalApi
public class MarshalInputStream
        extends AnnotatedObjectInputStream {
    private static final Logger _logger = LoggerFactory.getLogger(Constants.LOGGER_LRMI_MARSHAL);
    private static final int CODE_DISABLED = -1;
    private static final int CODE_NULL = 0;

    /**
     * maps keywords for primitive types and void to corresponding Class objects.
     */
    static final Map<String, Class<?>> _specialClasses = new HashMap<String, Class<?>>();

    static {
        _specialClasses.put("boolean", boolean.class);
        _specialClasses.put("byte", byte.class);
        _specialClasses.put("char", char.class);
        _specialClasses.put("short", short.class);
        _specialClasses.put("int", int.class);
        _specialClasses.put("long", long.class);
        _specialClasses.put("float", float.class);
        _specialClasses.put("double", double.class);
        _specialClasses.put("void", void.class);
    }

    private final Context _context;

    /**
     * @param in input stream to wrap.
     */
    public MarshalInputStream(InputStream in)
            throws IOException {
        this(in, new Context());
    }

    public MarshalInputStream(InputStream in, Context context)
            throws IOException {
        super(in);
        _context = context;
    }

    public static Context createContext() {
        return new Context();
    }

    /**
     * Reads a repetitive object which was written by {@link MarshalOutputStream#writeRepetitiveObject}.
     */
    public Object readRepetitiveObject()
            throws IOException, ClassNotFoundException {
        // Read object code:
        int code = readInt();
        // If null return:
        if (code == CODE_NULL)
            return null;
        // If repetitive optimization was disabled:
        if (code == CODE_DISABLED)
            return readObject();

        // Look for cached object by code:
        Object value = _context.getRepetitiveObjectsCache().get(code);

        if (value != null)
            return value;

        // If object is not cached, read it from the stream and cache it:
        value = readObject();
        // If object is string, intern it:
        if (value instanceof String)
            value = ((String) value).intern();
        _context.getRepetitiveObjectsCache().put(code, value);

        return value;
    }

    public Object readSmartExternalizable() throws IOException, ClassNotFoundException {
        // Read object code:
        int code = readInt();
        // If null return:
        if (code == CODE_NULL)
            return null;
        // If repetitive optimization was disabled:
        if (code == CODE_DISABLED)
            return readObject();

        // Look for cached constructor by code:
        IConstructor<?> ctor = _context._externalizableCtorCache.get(code);
        // If object constructor is not cached, read it from the stream and cache it for next time:
        if (ctor == null) {
            Class<?> clazz = (Class<?>) readObject();
            ctor = ReflectionUtil.createCtor(clazz);
            _context._externalizableCtorCache.put(code, ctor);
        }
        // Instantiate object using cached factory which invokes default constructor:
        Object value = ctor.newInstance();
        // Deserialize object using standard Externalizable:
        ((Externalizable)value).readExternal(this);
        return value;
    }

    /**
     * Resolves the appropriate {@link Class} object for the stream class descriptor
     * <code>classDesc</code>.
     *
     * <p><code>MarshalInputStream</code> implements this method as follows:
     *
     * <p>First tries to check if this class descriptor was already resolved if it does return the
     * class.
     *
     * <p>Else invokes the super class implementation.
     *
     * @param classDesc the stream class descriptor to resolve
     * @return the resolved class
     * @throws IOException            if <code>readAnnotation</code> throws an <code>IOException</code>,
     *                                or if <code>ClassLoading.loadClass</code> throws a
     *                                <code>MalformedURLException</code>
     * @throws ClassNotFoundException if <code>readAnnotation</code> or <code>ClassLoading.loadClass</code>
     *                                throws a <code>ClassNotFoundException</code>
     * @throws NullPointerException   if <code>classDesc</code> is <code>null</code>
     **/
    @Override
    protected Class<?> resolveClass(ObjectStreamClass classDesc)
            throws IOException, ClassNotFoundException {
        Class<?> resolvedClass = _context.getResolvedClass(classDesc);// check if already resolved        

        if (resolvedClass != null)
            return resolvedClass;

        try {
            boolean containsAnnotation = _context.containsAnnotation(classDesc);
            String annotation = null;
            if (containsAnnotation)
                annotation = _context.getAnnotation(classDesc);
            resolvedClass = super.resolveClass(classDesc, !containsAnnotation, annotation);

        } catch (ClassNotFoundException e) {
            String name = classDesc.getName();
            resolvedClass = _specialClasses.get(name);

            if (resolvedClass == null) {
                throw e;
            }
        }

        _context.putResolvedClass(classDesc, resolvedClass); // save resolved class

        return resolvedClass;
    }

    @Override
    protected String readAnnotation(ObjectStreamClass desc) throws IOException,
            ClassNotFoundException {
        String annotation = super.readAnnotation(desc);
        _context.putAnnotation(desc, annotation);
        return annotation;
    }


    /**
     * Resolves the appropriate {@link Class} object for the proxy class described by the interface
     * names <code>interfaceNames</code>.
     *
     * <p><code>MarshalInputStream</code> implements this method as follows:
     *
     * <p>First tries to check if this class descriptor was already resolved if it does return the
     * class.
     *
     * <p>Else invokes the super class implementation
     *
     * @param interfaceNames the list of interface names that were deserialized in the proxy class
     *                       descriptor
     * @return the resolved dynamic proxy class
     * @throws IOException            if <code>readAnnotation</code> throws an <code>IOException</code>,
     *                                or if <code>ClassLoading.loadProxyClass</code> throws a
     *                                <code>MalformedURLException</code>
     * @throws ClassNotFoundException if <code>readAnnotation</code> or <code>ClassLoading.loadProxyClass</code>
     *                                throws a <code>ClassNotFoundException</code>
     * @throws NullPointerException   if <code>interfaceNames</code> is <code>null</code> or if any
     *                                element of <code>interfaceNames</code> is <code>null</code>
     **/
    @Override
    protected Class<?> resolveProxyClass(String[] interfaceNames)
            throws IOException, ClassNotFoundException {
        NamesWrapper names = new NamesWrapper(interfaceNames);

        Class<?> cl = _context.getClassFromAnnotateInterfaceNameMap(names);
        if (cl != null)
            return cl;

        //load the proxy class
        cl = super.resolveProxyClass(interfaceNames);

        if (cl != null)
            _context.putAnnotateInterfaceNamesMap(names, cl);
        return cl;
    }


    /**
     * Overrides the super method and tries to get the class descriptor from a local map. If not
     * found read from the stream and save to local map.
     *
     * @return class descriptor
     */
    @Override
    protected ObjectStreamClass readClassDescriptor()
            throws IOException, ClassNotFoundException {
        ObjectStreamClass cl = null;

        int index = readInt();

        if (index != -1)
            cl = _context.getObjectStreamClass(index);

        if (cl == null) {
            if (_logger.isTraceEnabled()) {
                Long classLoaderKey = ClassLoaderCache.getCache().getClassLoaderKey(ClassLoaderHelper.getContextClassLoader());
                logFinest("Received new incoming ObjectStreamClass with key " + index + ", context class loader key " + classLoaderKey + ", reading it from stream");
            }
            cl = super.readClassDescriptor();
            if (index != -1)
                _context.addObjectStreamClass(index, cl);
        }

        return cl;
    }

    @Override
    protected void readStreamHeader() throws IOException {
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

        /**
         * maps IDs (seq' number) to ObjectStreamClass.
         */
        //This map is kept at the context level without class loading association because the MarshalOutputStream
        //is not aware of which context class loader is reading from the MarshalInputStream and as a result it
        //assign keys to the ObjectStreamClass globally for all class loaders.
        //This should not cause ClassLoader leak because the ObjectStreamClass is not supposed to hold a reference to
        //the actual class in the input stream
        private final ISafeArray<ObjectStreamClass> classMap = new SafeArray<ObjectStreamClass>();
        private final HashMap<ObjectStreamClass, String> annotationMap = new HashMap<ObjectStreamClass, String>();

        private final static ClassLoaderContext REMOVED_CONTEXT_MARKER = new ClassLoaderContext(-2L);
        private final static long NULL_CL_MARKER = -1;
        private final IntegerObjectMap<Object> _repetitiveObjectsCache = CollectionsFactory.getInstance().createIntegerObjectMap();
        public final IntegerObjectMap<IConstructor<?>> _externalizableCtorCache = CollectionsFactory.getInstance().createIntegerObjectMap();


        public IntegerObjectMap<Object> getRepetitiveObjectsCache() {
            return _repetitiveObjectsCache;
        }

        public synchronized void addObjectStreamClass(int index, ObjectStreamClass cl) {
            classMap.add(index, cl);
            if (_logger.isTraceEnabled())
                logFinest("Adding new ObjectStreamClass to incoming context [" + cl.getName() + "] with specified key " + index + ", context class loader key " + ClassLoaderCache.getCache().getClassLoaderKey(ClassLoaderHelper.getContextClassLoader()));
        }

        public synchronized String getAnnotation(ObjectStreamClass classDesc) {
            return annotationMap.get(classDesc);
        }

        public synchronized boolean containsAnnotation(ObjectStreamClass classDesc) {
            return annotationMap.containsKey(classDesc);
        }

        public synchronized void putAnnotation(ObjectStreamClass desc, String annotation) {
            annotationMap.put(desc, annotation);
        }

        public synchronized ObjectStreamClass getObjectStreamClass(int index) {
            return classMap.get(index);
        }

        public synchronized void putAnnotateInterfaceNamesMap(NamesWrapper names,
                                                              Class<?> cl) {
            ClassLoaderContext classLoaderContext = getClassLoaderContext(true);
            classLoaderContext.putAnnotateInterfaceNamesMap(names, cl);

            if (_logger.isTraceEnabled())
                logFinest("Adding new NamesWrapper to incoming context [" + names + "] for class " + cl.getName() + ", context class loader key " + classLoaderContext.getClassLoaderCacheKey());
        }

        public synchronized Class<?> getClassFromAnnotateInterfaceNameMap(NamesWrapper names) {
            ClassLoaderContext classLoaderContext = getClassLoaderContext(false);
            if (classLoaderContext == null)
                return null;
            return classLoaderContext.getClassFromAnnotateInterfaceNameMap(names);
        }

        public synchronized void putResolvedClass(ObjectStreamClass classDesc, Class<?> cl) {
            ClassLoaderContext classLoaderContext = getClassLoaderContext(true);
            classLoaderContext.putResolvedClass(classDesc, cl);

            if (_logger.isTraceEnabled())
                logFinest("Adding new resolved class to incoming context [" + cl.getName() + "] for ObjectStreamClass " + classDesc.getName() + ", context class loader key " + classLoaderContext.getClassLoaderCacheKey());
        }

        public synchronized Class<?> getResolvedClass(ObjectStreamClass classDesc) {
            ClassLoaderContext classLoaderContext = getClassLoaderContext(false);
            if (classLoaderContext == null)
                return null;
            return classLoaderContext.getResolvedClass(classDesc);
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
                logFinest("Removed class loader [" + classLoaderKey + "] related context from marshal input stream, explicit=" + explicit);
            _classLoaderContextMap.put(classLoaderKey, REMOVED_CONTEXT_MARKER);
        }

        public synchronized void close() {
            if (_logger.isTraceEnabled())
                logFinest("Closing marshal input stream context");
            for (Long clKey : _classLoaderContextMap.keySet()) {
                _classLoaderContextMap.put(clKey, REMOVED_CONTEXT_MARKER);
                ClassLoaderCache.getCache().removeClassLoaderStateListener(clKey, this);
            }
        }

        public synchronized void reset() {
            if (_logger.isTraceEnabled())
                logFinest("Resetting entire marshal input stream context");
            for (Long clKey : _classLoaderContextMap.keySet()) {
                _classLoaderContextMap.put(clKey, REMOVED_CONTEXT_MARKER);
                ClassLoaderCache.getCache().removeClassLoaderStateListener(clKey, this);
            }
            _classLoaderContextMap.clear();
            classMap.clear();
            annotationMap.clear();
            _repetitiveObjectsCache.clear();
            _externalizableCtorCache.clear();
        }

        /**
         * Context per class loader
         *
         * @author eitany
         * @since 7.0.1
         */
        private static class ClassLoaderContext {
            /**
             * maps ObjectStreamClass to resolved classes.
             */
            private final HashMap<ObjectStreamClass, Class<?>> resolvedClassesMap = new HashMap<ObjectStreamClass, Class<?>>();
            /**
             * maps interfaces arrays to resolved classes.
             */
            private final HashMap<NamesWrapper, Class<?>> annotateInterfaceNamesMap = new HashMap<NamesWrapper, Class<?>>();
            private final Long clKey;

            public ClassLoaderContext(Long clKey) {
                this.clKey = clKey;
            }

            public Long getClassLoaderCacheKey() {
                return clKey;
            }

            public void putResolvedClass(ObjectStreamClass classDesc,
                                         Class<?> cl) {
                resolvedClassesMap.put(classDesc, cl);
            }

            public Class<?> getResolvedClass(ObjectStreamClass classDesc) {
                return resolvedClassesMap.get(classDesc);
            }

            public Class<?> getClassFromAnnotateInterfaceNameMap(NamesWrapper names) {
                return annotateInterfaceNamesMap.get(names);
            }

            public void putAnnotateInterfaceNamesMap(NamesWrapper names,
                                                     Class<?> cl) {
                annotateInterfaceNamesMap.put(names, cl);
            }
        }

    }

    /**
     * Wrap array of interfaces names in order to be used as key to _annotateInterfaceNamesMap.
     *
     * @author Guy Korland
     * @version 1.0
     * @since 5.01
     */
    static final class NamesWrapper {
        final private String[] _interfaceNames;
        final private int _hashCode;

        public NamesWrapper(String[] interfaceNames) {
            _interfaceNames = interfaceNames;
            if (_interfaceNames.length > 0) {    //Calculates the hashcode from the first String
                _hashCode = _interfaceNames[0].hashCode();
            } else // if no first string then a random number
            {
                _hashCode = 4325349;
            }
        }

        /**
         * Compares the two names arrays.
         *
         * @param wrapper another NamesWrapper to compare
         * @return <code>true</code> if equals
         */
        @Override
        public boolean equals(Object wrapper) {
            return Arrays.equals(_interfaceNames, ((NamesWrapper) wrapper)._interfaceNames);
        }

        /**
         * Returns the calculated hashCode
         *
         * @return the hash code.
         */
        @Override
        public int hashCode() {
            return _hashCode;
        }

        @Override
        public String toString() {
            return Arrays.toString(_interfaceNames);
        }
    }

}
