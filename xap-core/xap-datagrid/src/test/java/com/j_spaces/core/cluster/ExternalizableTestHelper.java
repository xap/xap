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

/*
 * @(#)ExternalizableTestHelper.java   Jan 1, 2008
 *
 * Copyright 2007 GigaSpaces Technologies Inc.
 */
package com.j_spaces.core.cluster;

import java.io.Externalizable;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Fill objects properties and setters with values. Used to create none empty object that suitable
 * for Externalizeble tests
 *
 * @author Barak Bar Orion
 * @since 6.1
 */
@com.gigaspaces.api.InternalApi
public class ExternalizableTestHelper {
    private Random random = new Random();

    public <T> T fill(T object) throws Exception {
        for (Field field : object.getClass().getDeclaredFields()) {
            if (shouldIgnore(field)) {
                continue;
            }
            if (!Modifier.isFinal(field.getModifiers()) && !Modifier.isTransient(field.getModifiers())) {
                if (isPrimitive(field.getType())) {
                    // jave primitive
                    setFieldValue(field, object, createObjectOfType(field.getType()));
                } else if (List.class.isAssignableFrom(field.getType())) {
                    Type type = field.getGenericType();
                    if (type instanceof ParameterizedType) {
                        // generic list
                        ParameterizedType genericType = (ParameterizedType) type;
                        Class argType = (Class) genericType.getActualTypeArguments()[0];
                        Object arg = createObjectOfType(argType);
                        setFieldValue(field, object, createNewList(field, arg));
                    } else {
                        // untyped list
                        setFieldValue(field, object, createNewList(field, 1L));
                    }
                } else if (Set.class.isAssignableFrom(field.getType())) {
                    Type type = field.getGenericType();
                    if (type instanceof ParameterizedType) {
                        // generic list
                        ParameterizedType genericType = (ParameterizedType) type;
                        Class argType = (Class) genericType.getActualTypeArguments()[0];
                        Object arg = createObjectOfType(argType);
                        setFieldValue(field, object, createNewSet(field, arg));
                    } else {
                        // untyped list
                        setFieldValue(field, object, createNewSet(field, 1L));
                    }
                } else if (Map.class.isAssignableFrom(field.getType())) {
                    Type type = field.getGenericType();
                    if (type instanceof ParameterizedType) {
                        // generic list
                        ParameterizedType genericType = (ParameterizedType) type;
                        Class argType1 = (Class) genericType.getActualTypeArguments()[0];
                        Object arg1 = createObjectOfType(argType1);
                        Type argType2 = genericType.getActualTypeArguments()[1];
                        Object arg2 = null;
                        if (argType2 instanceof ParameterizedType) {
                            ParameterizedType type2 = (ParameterizedType) argType2;
                            // TODO fill this (Map<String, List<String>) for example
                            arg1 = null;
                        } else {
                            arg2 = createObjectOfType((Class) argType2);
                        }
                        if (arg1 != null) {
                            setFieldValue(field, object, createNewMap(field, arg1, arg2));
                        }
                    } else {
                        // untyped list
                        setFieldValue(field, object, createNewMap(field, "key", "value"));
                    }
                } else // an object
                {
                    try {
                        Constructor constructor = field.getType().getConstructor();
                        constructor.setAccessible(true);
                        setFieldValue(field, object, fill(constructor.newInstance()));
                    } catch (Exception ex) {/* field without a default Ctor*/
                        if (Externalizable.class.isAssignableFrom(field.getType()))
                            throw ex;
                    }
                }
            }
        }
        return object;
    }

    private <T> boolean isPrimitive(Class type) {
        return typeOneOf(type, boolean.class, Boolean.class, int.class, Integer.class, long.class,
                Long.class, byte.class, Byte.class, short.class, Short.class, char.class, Character.class,
                float.class, Float.class, double.class, Double.class, String.class);
    }

    private boolean shouldIgnore(Field field) {
        return fieldOneOf(field, Logger.class);
    }

    private boolean fieldOneOf(Field field, Class... classes) {
        return typeOneOf(field.getType(), classes);
    }

    private boolean typeOneOf(Class type, Class... classes) {
        for (Class aClass : classes) {
            if (type.equals(aClass))
                return true;
        }
        return false;
    }

    private Object createObjectOfType(Class type) throws IllegalAccessException, InstantiationException, NoSuchMethodException, InvocationTargetException {
        if (type.equals(boolean.class) || type.equals(Boolean.class))
            return random.nextBoolean();
        else if (type.equals(int.class) || type.equals(Integer.class))
            return random.nextInt();
        else if (type.equals(long.class) || type.equals(Long.class))
            return random.nextLong();
        else if (type.equals(byte.class) || type.equals(Byte.class))
            return (byte) random.nextInt();
        else if (type.equals(short.class) || type.equals(Short.class))
            return (short) random.nextInt();
        else if (type.equals(char.class) || type.equals(Character.class))
            return (char) random.nextInt();
        else if (type.equals(float.class) || type.equals(Float.class))
            return random.nextFloat();
        else if (type.equals(double.class) || type.equals(Double.class))
            return random.nextDouble();
        else if (type.equals(String.class))
            return "" + random.nextLong();
        else if (Enum.class.isAssignableFrom(type)) {
            return values(type).iterator().next();
        }
        Constructor c = findDefaultConstructor(type);
        c.setAccessible(true);
        return c.newInstance();
    }

    public static <T extends Enum<T>> Collection<T> values(Class<T> enumClass) {
        try {
            Method valuesMethod = enumClass.getMethod("values", new Class[0]);
            T[] values = (T[]) valuesMethod.invoke(null, new Object[0]);
            return Arrays.asList(values);
        } catch (Exception ex) {
            throw new RuntimeException("Exceptions here should be impossible", ex);
        }
    }

    private Constructor findDefaultConstructor(Class type) {
        Constructor[] constructors = type.getDeclaredConstructors();
        for (Constructor c : constructors) {
            if (c.getParameterTypes() == null || c.getParameterTypes().length == 0) {
                return c;
            }
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    private List createNewList(Field field, Object withValue)
            throws InstantiationException, IllegalAccessException {
        List l;
        try {
            l = (List) field.getType().newInstance();
            l.add(withValue);
            return l;
        } catch (InstantiationException e) {
        } catch (IllegalAccessException e) {
        }
        l = new ArrayList();
        l.add(withValue);
        return l;
    }

    private Set createNewSet(Field field, Object withValue)
            throws InstantiationException, IllegalAccessException {
        Set l;
        try {
            l = (Set) field.getType().newInstance();
            l.add(withValue);
            return l;
        } catch (InstantiationException e) {
        } catch (IllegalAccessException e) {
        }
        l = new HashSet();
        l.add(withValue);
        return l;
    }

    private Map createNewMap(Field field, Object key, Object value)
            throws InstantiationException, IllegalAccessException {
        Map l;
        try {
            l = (Map) field.getType().newInstance();
            l.put(key, value);
            return l;
        } catch (InstantiationException e) {
        } catch (IllegalAccessException e) {
        }
        l = new HashMap();
        l.put(key, value);
        return l;
    }

    private <T> void setFieldValue(Field field, T object, Object value) {
        try {
            field.setAccessible(true);
            field.set(object, value);
        } catch (IllegalAccessException e) {
            System.out.println(e);
        }
    }

    public <T> boolean areEquals(T object1, T object2) {
        if (object1 == object2)
            return true;

        return areAllFieldsEquals(object1, object2);
    }

    private <T> boolean areAllFieldsEquals(T object1, T object2) {
        if (object1.getClass().equals(Object.class)) {
            return true;
        }
        for (Field field1 : object1.getClass().getDeclaredFields()) {
            try {
                field1.setAccessible(true);
                if (shouldIgnore(field1)) {
                    return true;
                }
                Object value1 = field1.get(object1);
                try {
                    Field field2 = object2.getClass().getDeclaredField(field1.getName());
                    field2.setAccessible(true);
                    Object value2 = field2.get(object2);
                    if ((value1 == null) && (value2 == null)) {
                        continue;
                    } else if ((value1 == null) || (value2 == null)) {
                        System.out.println("Equals returns error on field " + field1.getName() + " values are: " + value1 + ", " + value2);
                        return false;
                    } else if (isPrimitive(field1.getType())) {
                        if (value1.equals(value2)) {
                            continue;
                        } else {
                            System.out.println("Equals returns error on field " + field1.getName() + " values are: [" + value1 + "], [" + value2 + "]");
                            return false;
                        }
                    } else if (Collection.class.isAssignableFrom(field1.getType())) {
                        Collection col1 = (Collection) value1;
                        Collection col2 = (Collection) value2;
                        if (col1.size() != col2.size()) {
                            System.out.println("Equals returns error on field " + field1.getName() + " values are: [" + value1 + "], [" + value2 + "]");
                            return false;
                        }
                        Iterator it1 = col1.iterator();
                        Iterator it2 = col2.iterator();
                        while (it1.hasNext()) {
                            it2.hasNext();
                            Object colVal1 = it1.next();
                            Object colVal2 = it2.next();
                            if (isPrimitive(colVal1.getClass())) {
                                if (!colVal1.equals(colVal2)) {
                                    return false;
                                }
                            } else {
                                if (!areEquals(colVal1, colVal2)) {
                                    return false;
                                }
                            }
                        }
                        return true;
                    } else {
                        if (areEquals(value1, value2)) {
                            continue;
                        } else {
                            System.out.println("Equals returns error on field " + field1.getName() + " values are: [" + value1 + "], [" + value2 + "]");
                            return false;
                        }
                    }
                } catch (NoSuchFieldException e) {
                    System.out.println(e);
                } catch (IllegalAccessException e) {
                    System.out.println(e);
                }
                return false;
            } catch (IllegalAccessException e) {
                System.out.println(e);
            }
        }
        return true;
    }

    /**
     * return true iff the first equals defined first time in Object when searching for it from
     * current value..
     *
     * @param c the given type
     */
    private boolean equalsDefinedFirstOnObject(Class c) {
        if (c.equals(Object.class)) {
            return true;
        }
        try {
            Method m = c.getMethod("equals", Object.class);
            if (m.getDeclaringClass().equals(Object.class)) {
                return true;
            }
            if (Modifier.isPublic(m.getModifiers()) && m.getReturnType().equals(boolean.class)) {
                return false;
            }
        } catch (NoSuchMethodException e) {
        }
        return equalsDefinedFirstOnObject(c.getSuperclass());
    }

}
