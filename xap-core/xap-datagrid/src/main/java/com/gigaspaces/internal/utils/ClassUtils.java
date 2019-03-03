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

package com.gigaspaces.internal.utils;

import java.beans.Introspector;
import java.lang.reflect.Array;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.*;

/**
 * Miscellaneous class utility methods. Mainly for internal use within the framework; consider
 * Jakarta's Commons Lang for a more comprehensive suite of utilities.
 *
 * @author kimchy
 */
public abstract class ClassUtils {

    /**
     * Suffix for array class names
     */
    public static final String ARRAY_SUFFIX = "[]";

    /**
     * The package separator character '.'
     */
    private static final char PACKAGE_SEPARATOR_CHAR = '.';

    /**
     * The inner class separator character '$'
     */
    private static final char INNER_CLASS_SEPARATOR_CHAR = '$';

    /**
     * The CGLIB class separator character "$$"
     */
    private static final String CGLIB_CLASS_SEPARATOR_CHAR = "$$";

    /**
     * Map with primitive wrapper type as key and corresponding primitive type as value, for
     * example: Integer.class -> int.class.
     */
    private static final Map<Class, Class> primitiveWrapperTypeMap = new HashMap<Class, Class>(8);

    /**
     * Map with primitive type name as key and corresponding primitive type as value, for example:
     * "int" -> "int.class".
     */
    private static final Map<String, Class> primitiveTypeNameMap = new HashMap<String, Class>(8);

    static {
        primitiveWrapperTypeMap.put(Boolean.class, boolean.class);
        primitiveWrapperTypeMap.put(Byte.class, byte.class);
        primitiveWrapperTypeMap.put(Character.class, char.class);
        primitiveWrapperTypeMap.put(Double.class, double.class);
        primitiveWrapperTypeMap.put(Float.class, float.class);
        primitiveWrapperTypeMap.put(Integer.class, int.class);
        primitiveWrapperTypeMap.put(Long.class, long.class);
        primitiveWrapperTypeMap.put(Short.class, short.class);

        for (Class primitiveClass : primitiveWrapperTypeMap.values()) {
            primitiveTypeNameMap.put(primitiveClass.getName(), primitiveClass);
        }
    }


    /**
     * Return a default ClassLoader to use (never <code>null</code>). Returns the thread context
     * ClassLoader, if available. The ClassLoader that loaded the ClassUtils class will be used as
     * fallback. <p>Call this method if you intend to use the thread context ClassLoader in a
     * scenario where you absolutely need a non-null ClassLoader reference: for example, for class
     * path resource loading (but not necessarily for <code>Class.forName</code>, which accepts a
     * <code>null</code> ClassLoader reference as well).
     *
     * @see Thread#getContextClassLoader()
     */
    public static ClassLoader getDefaultClassLoader() {
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        if (cl == null) {
            // No thread context class loader -> use class loader of this class.
            cl = ClassUtils.class.getClassLoader();
        }
        return cl;
    }

    /**
     * Return whether the {@link Class} identified by the supplied name is present and can be
     * loaded. Will return <code>false</code> if either the class or one of its dependencies is not
     * present or cannot be loaded.
     */
    public static boolean isPresent(String className) {
        try {
            forName(className);
            return true;
        } catch (Throwable ex) {
            return false;
        }
    }

    /**
     * Replacement for <code>Class.forName()</code> that also returns Class instances for primitives
     * (like "int") and array class names (like "String[]"). <p>Always uses the default class
     * loader: that is, preferably the thread context class loader, or the ClassLoader that loaded
     * the ClassUtils class as fallback.
     *
     * @param name the name of the Class
     * @return Class instance for the supplied name
     * @see Class#forName(String, boolean, ClassLoader)
     * @see #getDefaultClassLoader()
     */
    public static Class forName(String name) throws ClassNotFoundException {
        return forName(name, getDefaultClassLoader());
    }

    /**
     * Replacement for <code>Class.forName()</code> that also returns Class instances for primitives
     * (like "int") and array class names (like "String[]").
     *
     * @param name        the name of the Class
     * @param classLoader the class loader to use
     * @return Class instance for the supplied name
     * @see Class#forName(String, boolean, ClassLoader)
     */
    public static Class forName(String name, ClassLoader classLoader) throws ClassNotFoundException {
        Class clazz = resolvePrimitiveClassName(name);
        if (clazz != null) {
            return clazz;
        }
        if (name.endsWith(ARRAY_SUFFIX)) {
            // special handling for array class names
            String elementClassName = name.substring(0, name.length() - ARRAY_SUFFIX.length());
            Class elementClass = ClassUtils.forName(elementClassName, classLoader);
            return Array.newInstance(elementClass, 0).getClass();
        }
        return Class.forName(name, true, classLoader);
    }

    /**
     * Resolve the given class name as primitive class, if appropriate.
     *
     * @param name the name of the potentially primitive class
     * @return the primitive class, or <code>null</code> if the name does not denote a primitive
     * class
     */
    public static Class resolvePrimitiveClassName(String name) {
        Class result = null;
        // Most class names will be quite long, considering that they
        // SHOULD sit in a package, so a length check is worthwhile.
        if (name != null && name.length() <= 8) {
            // Could be a primitive - likely.
            result = primitiveTypeNameMap.get(name);
        }
        return result;
    }

    /**
     * Get the class name without the qualified package name.
     *
     * @param className the className to get the short name for
     * @return the class name of the class without the package name
     * @throws IllegalArgumentException if the className is empty
     */
    public static String getShortName(String className) {
        if (!StringUtils.hasLength(className)) {
            throw new IllegalArgumentException("Class name must not be empty");
        }

        int lastDotIndex = className.lastIndexOf(PACKAGE_SEPARATOR_CHAR);
        int nameEndIndex = className.indexOf(CGLIB_CLASS_SEPARATOR_CHAR);
        if (nameEndIndex == -1) {
            nameEndIndex = className.length();
        }
        String shortName = className.substring(lastDotIndex + 1, nameEndIndex);
        return shortName.replace(INNER_CLASS_SEPARATOR_CHAR, PACKAGE_SEPARATOR_CHAR);
    }

    /**
     * Get the class name without the qualified package name.
     *
     * @param clazz the class to get the short name for
     * @return the class name of the class without the package name
     */
    public static String getShortName(Class clazz) {
        return getShortName(getQualifiedName(clazz));
    }

    /**
     * Return the short string name of a Java class in decapitalized JavaBeans property format.
     *
     * @param clazz the class
     * @return the short name rendered in a standard JavaBeans property format
     * @see java.beans.Introspector#decapitalize(String)
     */
    public static String getShortNameAsProperty(Class clazz) {
        return Introspector.decapitalize(getShortName(clazz));
    }

    /**
     * Return the qualified name of the given class: usually simply the class name, but component
     * type class name + "[]" for arrays.
     *
     * @param clazz the class
     * @return the qualified name of the class
     */
    public static String getQualifiedName(Class clazz) {
        Objects.requireNonNull(clazz, "Class must not be null");
        if (clazz.isArray()) {
            return getQualifiedNameForArray(clazz);
        } else {
            return clazz.getName();
        }
    }


    /**
     * Return the qualified name of the given method, consisting of fully qualified interface/class
     * name + "." + method name.
     *
     * @param method the method
     * @return the qualified name of the method
     */
    public static String getQualifiedMethodName(Method method) {
        Objects.requireNonNull(method, "Method must not be null");
        return method.getDeclaringClass().getName() + '.' + method.getName();
    }

    /**
     * Determine whether the given class has a method with the given signature. <p>Essentially
     * translates <code>NoSuchMethodException</code> to "false".
     *
     * @param clazz      the clazz to analyze
     * @param methodName the name of the method
     * @param paramTypes the parameter types of the method
     * @see java.lang.Class#getMethod
     */
    public static boolean hasMethod(Class clazz, String methodName, Class[] paramTypes) {
        return (getMethodIfAvailable(clazz, methodName, paramTypes) != null);
    }

    /**
     * Determine whether the given class has a method with the given signature, and return it if
     * available (else return <code>null</code>). <p>Essentially translates
     * <code>NoSuchMethodException</code> to <code>null</code>.
     *
     * @param clazz      the clazz to analyze
     * @param methodName the name of the method
     * @param paramTypes the parameter types of the method
     * @return the method, or <code>null</code> if not found
     * @see java.lang.Class#getMethod
     */
    public static Method getMethodIfAvailable(Class clazz, String methodName, Class[] paramTypes) {
        Objects.requireNonNull(clazz, "Class must not be null");
        Objects.requireNonNull(methodName, "Method name must not be null");
        try {
            return clazz.getMethod(methodName, paramTypes);
        } catch (NoSuchMethodException ex) {
            return null;
        }
    }

    /**
     * Return the number of methods with a given name (with any argument types), for the given class
     * and/or its superclasses. Includes non-public methods.
     *
     * @param clazz      the clazz to check
     * @param methodName the name of the method
     * @return the number of methods with the given name
     */
    public static int getMethodCountForName(Class clazz, String methodName) {
        Objects.requireNonNull(clazz, "Class must not be null");
        Objects.requireNonNull(methodName, "Method name must not be null");
        int count = 0;
        for (Method method : clazz.getDeclaredMethods()) {
            if (methodName.equals(method.getName())) {
                count++;
            }
        }
        Class[] ifcs = clazz.getInterfaces();
        for (Class ifc : ifcs) {
            count += getMethodCountForName(ifc, methodName);
        }
        Class superClass = clazz.getSuperclass();
        if (superClass != null) {
            count += getMethodCountForName(superClass, methodName);
        }
        return count;
    }

    /**
     * Does the given class and/or its superclasses at least have one or more methods (with any
     * argument types)? Includes non-public methods.
     *
     * @param clazz      the clazz to check
     * @param methodName the name of the method
     * @return whether there is at least one method with the given name
     */
    public static boolean hasAtLeastOneMethodWithName(Class clazz, String methodName) {
        Objects.requireNonNull(clazz, "Class must not be null");
        Objects.requireNonNull(methodName, "Method name must not be null");
        for (Method method : clazz.getDeclaredMethods()) {
            if (method.getName().equals(methodName)) {
                return true;
            }
        }
        Class[] ifcs = clazz.getInterfaces();
        for (Class ifc : ifcs) {
            if (hasAtLeastOneMethodWithName(ifc, methodName)) {
                return true;
            }
        }
        Class superClass = clazz.getSuperclass();
        return superClass != null && hasAtLeastOneMethodWithName(superClass, methodName);
    }

    /**
     * Return a static method of a class.
     *
     * @param methodName the static method name
     * @param clazz      the class which defines the method
     * @param args       the parameter types to the method
     * @return the static method, or <code>null</code> if no static method was found
     * @throws IllegalArgumentException if the method name is blank or the clazz is null
     */
    public static Method getStaticMethod(Class clazz, String methodName, Class[] args) {
        Objects.requireNonNull(clazz, "Class must not be null");
        Objects.requireNonNull(methodName, "Method name must not be null");
        try {
            Method method = clazz.getDeclaredMethod(methodName, args);
            if ((method.getModifiers() & Modifier.STATIC) != 0) {
                return method;
            }
        } catch (NoSuchMethodException ex) {
            // do nothing
        }
        return null;
    }


    /**
     * Check if the given class represents a primitive wrapper, i.e. Boolean, Byte, Character,
     * Short, Integer, Long, Float, or Double.
     */
    public static boolean isPrimitiveWrapper(Class clazz) {
        Objects.requireNonNull(clazz, "Class must not be null");
        return primitiveWrapperTypeMap.containsKey(clazz);
    }

    /**
     * Check if the given class represents a primitive (i.e. boolean, byte, char, short, int, long,
     * float, or double) or a primitive wrapper (i.e. Boolean, Byte, Character, Short, Integer,
     * Long, Float, or Double).
     */
    public static boolean isPrimitiveOrWrapper(Class clazz) {
        Objects.requireNonNull(clazz, "Class must not be null");
        return (clazz.isPrimitive() || isPrimitiveWrapper(clazz));
    }

    /**
     * Check if the given class represents an array of primitives, i.e. boolean, byte, char, short,
     * int, long, float, or double.
     */
    public static boolean isPrimitiveArray(Class clazz) {
        Objects.requireNonNull(clazz, "Class must not be null");
        return (clazz.isArray() && clazz.getComponentType().isPrimitive());
    }

    /**
     * Check if the given class represents an array of primitive wrappers, i.e. Boolean, Byte,
     * Character, Short, Integer, Long, Float, or Double.
     */
    public static boolean isPrimitiveWrapperArray(Class clazz) {
        Objects.requireNonNull(clazz, "Class must not be null");
        return (clazz.isArray() && isPrimitiveWrapper(clazz.getComponentType()));
    }

    /**
     * Determine if the given target type is assignable from the given value type, assuming setting
     * by reflection. Considers primitive wrapper classes as assignable to the corresponding
     * primitive types.
     *
     * @param targetType the target type
     * @param valueType  the value type that should be assigned to the target type
     * @return if the target type is assignable from the value type
     */
    public static boolean isAssignable(Class targetType, Class valueType) {
        Objects.requireNonNull(targetType, "Target type must not be null");
        Objects.requireNonNull(valueType, "Value type must not be null");
        return (targetType.isAssignableFrom(valueType) ||
                targetType.equals(primitiveWrapperTypeMap.get(valueType)));
    }

    /**
     * Determine if the given type is assignable from the given value, assuming setting by
     * reflection. Considers primitive wrapper classes as assignable to the corresponding primitive
     * types.
     *
     * @param type  the target type
     * @param value the value that should be assigned to the type
     * @return if the type is assignable from the value
     */
    public static boolean isAssignableValue(Class type, Object value) {
        Objects.requireNonNull(type, "Type must not be null");
        return (value != null ? isAssignable(type, value.getClass()) : !type.isPrimitive());
    }


    /**
     * Return a path suitable for use with <code>ClassLoader.getResource</code> (also suitable for
     * use with <code>Class.getResource</code> by prepending a slash ('/') to the return value.
     * Built by taking the package of the specified class file, converting all dots ('.') to slashes
     * ('/'), adding a trailing slash if necessary, and concatenating the specified resource name to
     * this. <br/>As such, this function may be used to build a path suitable for loading a resource
     * file that is in the same package as a class file, although {@link
     * org.springframework.core.io.ClassPathResource} is usually even more convenient.
     *
     * @param clazz        the Class whose package will be used as the base
     * @param resourceName the resource name to append. A leading slash is optional.
     * @return the built-up resource path
     * @see ClassLoader#getResource
     * @see Class#getResource
     */
    public static String addResourcePathToPackagePath(Class clazz, String resourceName) {
        Objects.requireNonNull(resourceName, "Resource name must not be null");
        if (!resourceName.startsWith("/")) {
            return classPackageAsResourcePath(clazz) + '/' + resourceName;
        }
        return classPackageAsResourcePath(clazz) + resourceName;
    }

    /**
     * Given an input class object, return a string which consists of the class's package name as a
     * pathname, i.e., all dots ('.') are replaced by slashes ('/'). Neither a leading nor trailing
     * slash is added. The result could be concatenated with a slash and the name of a resource, and
     * fed directly to ClassLoader.getResource(). For it to be fed to Class.getResource, a leading
     * slash would also have to be prepended to the return value.
     *
     * @param clazz the input class. A null value or the default (empty) package will result in an
     *              empty string ("") being returned.
     * @return a path which represents the package name
     * @see ClassLoader#getResource
     * @see Class#getResource
     */
    public static String classPackageAsResourcePath(Class clazz) {
        if (clazz == null || clazz.getPackage() == null) {
            return "";
        }
        return clazz.getPackage().getName().replace('.', '/');
    }

    /**
     * Return all interfaces that the given object implements as array, including ones implemented
     * by superclasses.
     *
     * @param object the object to analyze for interfaces
     * @return all interfaces that the given object implements as array
     */
    public static Class[] getAllInterfaces(Object object) {
        Set<Class> interfaces = getAllInterfacesAsSet(object);
        return interfaces.toArray(new Class[interfaces.size()]);
    }

    /**
     * Return all interfaces that the given class implements as array, including ones implemented by
     * superclasses. <p>If the class itself is an interface, it gets returned as sole interface.
     *
     * @param clazz the class to analyze for interfaces
     * @return all interfaces that the given object implements as array
     */
    public static Class[] getAllInterfacesForClass(Class clazz) {
        Set<Class> interfaces = getAllInterfacesForClassAsSet(clazz);
        return interfaces.toArray(new Class[interfaces.size()]);
    }

    /**
     * Return all interfaces that the given object implements as List, including ones implemented by
     * superclasses.
     *
     * @param object the object to analyze for interfaces
     * @return all interfaces that the given object implements as List
     */
    public static Set<Class> getAllInterfacesAsSet(Object object) {
        return getAllInterfacesForClassAsSet(object.getClass());
    }

    /**
     * Return all interfaces that the given class implements as Set, including ones implemented by
     * superclasses. <p>If the class itself is an interface, it gets returned as sole interface.
     *
     * @param clazz the class to analyze for interfaces
     * @return all interfaces that the given object implements as Set
     */
    public static Set<Class> getAllInterfacesForClassAsSet(Class clazz) {
        if (clazz.isInterface()) {
            return Collections.singleton(clazz);
        }
        Set<Class> interfaces = new HashSet<Class>();
        while (clazz != null) {
            for (int i = 0; i < clazz.getInterfaces().length; i++) {
                Class ifc = clazz.getInterfaces()[i];
                interfaces.add(ifc);
            }
            clazz = clazz.getSuperclass();
        }
        return interfaces;
    }


    private static String getQualifiedNameForArray(Class clazz) {
        StringBuilder buffer = new StringBuilder();
        while (clazz.isArray()) {
            clazz = clazz.getComponentType();
            buffer.append(ClassUtils.ARRAY_SUFFIX);
        }
        buffer.insert(0, clazz.getName());
        return buffer.toString();
    }

    /**
     * @return the class of desired object or <code>null</code> if the obj is <code>null</code>
     **/
    public static Class getClass(Object obj) {
        if (obj != null)
            return obj.getClass();

        return null;
    }

    /**
     * @return the extracted package name from supplied class name, if className without package
     * returns an empty String.
     */
    public static String getPackage(String className) {
        int inx = className.lastIndexOf(".");

        return inx == -1 ? "" : className.substring(0, inx);
    }

    public static String getTypeDisplayName(String typeName) {
        if (typeName == null || typeName.length() == 0)
            return typeName;

        if (typeName.charAt(0) != '[')
            return typeName;

        if (typeName.equals("[B"))
            return byte.class.getName() + "[]";
        if (typeName.equals("[S"))
            return short.class.getName() + "[]";
        if (typeName.equals("[I"))
            return int.class.getName() + "[]";
        if (typeName.equals("[J"))
            return long.class.getName() + "[]";
        if (typeName.equals("[F"))
            return float.class.getName() + "[]";
        if (typeName.equals("[D"))
            return double.class.getName() + "[]";
        if (typeName.equals("[Z"))
            return boolean.class.getName() + "[]";
        if (typeName.equals("[C"))
            return char.class.getName() + "[]";

        if (typeName.startsWith("[L") && typeName.endsWith(";"))
            return getTypeDisplayName(typeName.substring(2, typeName.length() - 1) + "[]");
        return getTypeDisplayName(typeName.substring(1, typeName.length())) + "[]";
    }
}
