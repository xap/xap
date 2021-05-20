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

package com.gigaspaces.internal.reflection;

import com.gigaspaces.internal.metadata.SpaceTypeInfo;
import com.gigaspaces.internal.reflection.standard.StandardReflectionFactory;

import net.jini.core.entry.Entry;

import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@com.gigaspaces.api.InternalApi
public class ReflectionUtil {
    private static final Logger _logger = LoggerFactory.getLogger(com.gigaspaces.logger.Constants.LOGGER_REFLECTION);

    /**
     * Used to look up no-arg constructors.
     */
    private static final Class<?>[] noArg = new Class<?>[0];

    /**
     * Ignore anything that isn't a public non-static mutable field
     */
    private static final int IGNORE_MODES = (Modifier.TRANSIENT | Modifier.STATIC | Modifier.FINAL);

    private static final IReflectionFactory _reflectionFactory = initialize();

    private static IReflectionFactory initialize() {
        final Class<?> factoryClass = com.gigaspaces.internal.reflection.fast.ASMReflectionFactory.class;
        IReflectionFactory reflectionFactory = (IReflectionFactory) createInstanceWithOptionalDependencies(factoryClass);
        if (reflectionFactory == null) {
            _logger.warn("Failed to create reflection factory [" + factoryClass.getName() + "], falling back to standard reflection instead");
            reflectionFactory = new StandardReflectionFactory();
        }
        return reflectionFactory;
    }

    public static Object createInstanceWithOptionalDependencies(Class<?> factoryClass) {
        try {
            return factoryClass.newInstance();
        } catch (NoClassDefFoundError e) {
            return null;
        } catch (InstantiationException e) {
            throw new RuntimeException("Failed to create an instance of " + factoryClass.getName(), e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException("Failed to create an instance of " + factoryClass.getName(), e);
        }
    }

    public static Object createProxy(ClassLoader loader, Class<?>[] interfaces, ProxyInvocationHandler handler, boolean allowCache) {
        return _reflectionFactory.getProxy(loader, interfaces, handler, allowCache);
    }

    public static <T> IConstructor<T> createCtor(Class<T> type) {
        return createCtor(getDefaultConstructor(type));
    }

    public static <T> IConstructor<T> createCtor(Constructor<T> ctor) {
        return _reflectionFactory.getConstructor(ctor);
    }

    public static <T> IParamsConstructor<T> createParamsCtor(Constructor<T> ctor) {
        return _reflectionFactory.getParamsConstructor(ctor);
    }

    public static <T> IMethod<T> createMethod(Method method) {
        return _reflectionFactory.getMethod(method);
    }

    public static <T> IMethod<T> createMethod(ClassLoader classLoader, Method method) {
        return _reflectionFactory.getMethod(classLoader, method);
    }

    public static <T> IMethod<T>[] createMethods(Method[] methods) {
        return _reflectionFactory.getMethods(methods);
    }

    public static <T> IMethod<T>[] createMethods(ClassLoader classLoader, Method[] methods) {
        return _reflectionFactory.getMethods(classLoader, methods);
    }

    public static <T> IGetterMethod<T> createGetterMethod(Method method) {
        return _reflectionFactory.getGetterMethod(method);
    }

    public static <T> IGetterMethod<T> createGetterMethod(ClassLoader classLoader, Method method) {
        return _reflectionFactory.getGetterMethod(classLoader, method);
    }

    public static <T> ISetterMethod<T> createSetterMethod(Method method) {
        return _reflectionFactory.getSetterMethod(method);
    }

    public static <T> ISetterMethod<T> createSetterMethod(ClassLoader classLoader, Method method) {
        return _reflectionFactory.getSetterMethod(classLoader, method);
    }

    public static <T, F> IField<T, F> createField(Field field) {
        return _reflectionFactory.getField(field);
    }

    public static <T> IProperties<T> createFieldProperties(Class<T> declaringClass, Field[] fields) {
        return _reflectionFactory.getFieldProperties(declaringClass, fields);
    }

    public static <T> String[] getConstructorParametersNames(Constructor<T> ctor) {
        return _reflectionFactory.getConstructorParametersNames(ctor);
    }

    public static <T> IProperties<T> createProperties(SpaceTypeInfo typeInfo) {
        return _reflectionFactory.getProperties(typeInfo);
    }

    private static void sortClassFields(List<IField> fields) {
        /**
         * a comparator class that compares Field objects by name.
         * (a method level class - cool! could use anonymous inner class,
         * but seems overly hacky)
         */
        class FieldsComparator<T> implements Comparator<IField<T, ?>> {
            public int compare(IField<T, ?> f1, IField<T, ?> f2) {
                return f1.getName().compareTo(f2.getName());
            }
        }

        Collections.sort(fields, new FieldsComparator());
    }

    /**
     * Return the canonical sorted fields of the specified class. We define the order of canonical
     * sorted fields of class C as follows: <p/> If C is "java.lang.Object", it is the regular
     * sorting of C. Otherwise, let C1 be the superclass of C, then the canonical order is the
     * canonical order of C1 follow by the regular order of C. <p/> The definition is well defined
     * and independent of JVM/Platform. Moreover, it has the quality that fields of superclasses
     * appear before fields of subclasses.
     *
     * The legal fields are according to {@link #isUsableField(IField)}
     */
    public static List<IField> getCanonicalSortedFields(Class claz) {
        if (claz == Object.class)
            return new ArrayList<IField>();

        // use recursion to compute canonical order of super class
        List<IField> fieldsOfSuperClass = getCanonicalSortedFields(claz.getSuperclass());

        // find fields of claz that do not belong to superclass
        IField[] fields = _reflectionFactory.getFields(claz);
        HashSet<IField> fieldsOfSuperClassSet = new HashSet<IField>();
        fieldsOfSuperClassSet.addAll(fieldsOfSuperClass);

        List<IField> fieldsOfClaz = new ArrayList<IField>();
        for (IField field : fields) {
            if (isUsableField(field) && !fieldsOfSuperClassSet.contains(field)) {
                fieldsOfClaz.add(field);
            }
        }

        // sort subclass fields and combine result
        sortClassFields(fieldsOfClaz);
        List<IField> result = new ArrayList<IField>();
        result.addAll(fieldsOfSuperClass);
        result.addAll(fieldsOfClaz);

        return result;
    }

    public static boolean isPublic(Class<?> type) {
        return Modifier.isPublic(type.getModifiers());
    }

    public static void assertIsPublic(Class<?> type) {
        if (!isPublic(type))
            throw new IllegalArgumentException("Class '" + type.getName() + "' is not public.");
    }

    public static <T> Constructor<T> getDefaultConstructor(Class<T> type) {
        try {
            return type.getDeclaredConstructor(noArg);
        } catch (NoSuchMethodException e) {
            throw new IllegalArgumentException("Failed to get default constructor for class '" + type.getName() + "'", e);
        } catch (SecurityException e) {
            throw new IllegalArgumentException("Failed to get default constructor for class '" + type.getName() + "'", e);
        }
    }

    public static void assertHasDefaultConstructor(Class<?> type) {
        getDefaultConstructor(type);
    }

    /**
     * Check if the Object is pojo
     *
     * @param object the object
     * @return true if the object is pojo otherwise false
     */
    public static boolean isPojo(Object object) {
        if (object == null)
            return false;

        if (object instanceof Object[]) {
            if (object instanceof Entry[])
                return false;
            if (Array.getLength(object) == 0)
                return false;
            // check the first value, if it is entry, then we assume that the rest
            // are entries
            // and no need for conversion
            Object value = Array.get(object, 0);
            if (value == null || value instanceof Entry)
                return false;
        } else if (object instanceof Entry)
            return false;
        return true;
    }

    public static boolean isProxyClass(Class<?> clz) {
        return IDynamicProxy.class.isAssignableFrom(clz) || Proxy.isProxyClass(clz);
    }

    /**
     * A generic method retrieves the InvocationHandler of a Proxy or an AbstractProxy.
     */
    public static Object getInvocationHandler(Object obj) {
        if (obj instanceof IDynamicProxy)
            return ((IDynamicProxy) obj).getInvocatioHandler();

        if (Proxy.isProxyClass(obj.getClass()))
            return Proxy.getInvocationHandler(obj);

        throw new IllegalArgumentException("not a proxy instance");
    }

    /**
     * Return <code>true</code> if the field is to be used for the entry. That is, return
     * <code>true</code> if the field isn't <code>transient</code>, <code>static</code>, or
     * <code>final</code>.
     */
    private static boolean isUsableField(IField<?, ?> field) {
        if ((field.getModifiers() & IGNORE_MODES) != 0)
            return false;

        // If it isn't ignorable, it has to be an object of some kind
        if (field.getType().isPrimitive()) {
            throw new IllegalArgumentException("Primitive fields not allowed in an Entry. Cause primitive field: [ " + field + "]");
        }

        return true;
    }

    public static Set<Class> getAllInterfacesForClassAsSet(Class clazz) {
        Set<Class> interfaces = new LinkedHashSet<Class>();
        if (!clazz.isInterface()) {
            if (clazz.getSuperclass() != null)
                interfaces.addAll(getAllInterfacesForClassAsSet(clazz.getSuperclass()));
        } else {
            interfaces.add(clazz);
        }
        Class ifcs[] = clazz.getInterfaces();
        for (int i = 0; i < ifcs.length; i++) {
            interfaces.addAll(getAllInterfacesForClassAsSet(ifcs[i]));
        }

        return interfaces;
    }

    public static String getInternalName(final Class c) {
        return c.getName().replace('.', '/');
    }

    public static String getDescriptor(final Class c) {
        StringBuffer buf = new StringBuffer();
        getDescriptor(buf, c);
        return buf.toString();
    }

    private static void getDescriptor(final StringBuffer buf, final Class c) {
        Class d = c;
        while (true) {
            if (d.isPrimitive()) {
                char car;
                if (d == Integer.TYPE) {
                    car = 'I';
                } else if (d == Void.TYPE) {
                    car = 'V';
                } else if (d == Boolean.TYPE) {
                    car = 'Z';
                } else if (d == Byte.TYPE) {
                    car = 'B';
                } else if (d == Character.TYPE) {
                    car = 'C';
                } else if (d == Short.TYPE) {
                    car = 'S';
                } else if (d == Double.TYPE) {
                    car = 'D';
                } else if (d == Float.TYPE) {
                    car = 'F';
                } else /* if (d == Long.TYPE) */ {
                    car = 'J';
                }
                buf.append(car);
                return;
            } else if (d.isArray()) {
                buf.append('[');
                d = d.getComponentType();
            } else {
                buf.append('L');
                String name = d.getName();
                int len = name.length();
                for (int i = 0; i < len; ++i) {
                    char car = name.charAt(i);
                    buf.append(car == '.' ? '/' : car);
                }
                buf.append(';');
                return;
            }
        }
    }

    public static ClassLoader getClassTargetLoader(Class memberClass) {
        // If the class is loaded in the Common ClassLoader try to load it to the Common ClassLoader
        ClassLoader memberClassLoader = memberClass.getClassLoader();
        if (ReflectionUtil.class.getClassLoader().equals(memberClassLoader)) {
            return memberClassLoader;
        }

        // Otherwise tries to load the class in the context ClassLoader (usually the Service ClassLoader)
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        if (!isValidClassLoader(classLoader, memberClassLoader)) {
            classLoader = memberClassLoader;
        }
        return classLoader == null ? ReflectionUtil.class.getClassLoader() : classLoader;
    }

    /**
     * Validate the class loader, checks that its a sub-classloader of the member class-loader and
     * the current classloader.
     */
    private static boolean isValidClassLoader(final ClassLoader checkClassLoader, final ClassLoader memberClassLoader) {

        boolean valid = false;

        // Does the checkClassLoader is a sibling of memberClassLoader
        if (memberClassLoader == null) {
            valid = true;
        } else {
            for (ClassLoader classLoader = checkClassLoader; classLoader != null; classLoader = classLoader.getParent()) {
                if (classLoader == memberClassLoader)
                    valid = true;
            }
        }
        if (!valid)
            return false;

        // Does the checkClassLoader is a sibling of ASMFactoryUtils ClassLoader
        final ClassLoader thisClassLoader = ReflectionUtil.class.getClassLoader();
        for (ClassLoader classLoader = checkClassLoader; classLoader != null; classLoader = classLoader.getParent()) {
            if (classLoader == thisClassLoader)
                return true;
        }

        return false;
    }

    public static String getMethodDescriptor(final Method m) {
        Class[] parameters = m.getParameterTypes();
        StringBuffer buf = new StringBuffer();
        buf.append('(');
        for (int i = 0; i < parameters.length; ++i) {
            getDescriptor(buf, parameters[i]);
        }
        buf.append(')');
        getDescriptor(buf, m.getReturnType());
        return buf.toString();
    }
}
