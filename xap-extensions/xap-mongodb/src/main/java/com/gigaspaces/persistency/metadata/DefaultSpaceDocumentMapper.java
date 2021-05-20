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

package com.gigaspaces.persistency.metadata;

import com.gigaspaces.document.SpaceDocument;
import com.gigaspaces.internal.reflection.ISetterMethod;
import com.gigaspaces.metadata.SpaceDocumentSupport;
import com.gigaspaces.metadata.SpaceTypeDescriptor;
import com.gigaspaces.persistency.Constants;
import com.gigaspaces.persistency.error.SpaceMongoException;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBObject;

import org.bson.types.ObjectId;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.reflect.Array;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URI;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * class helper to map from mongo document type to SpaceDocument and vice versa
 *
 * @author Shadi Massalha
 */
public class DefaultSpaceDocumentMapper implements
        SpaceDocumentMapper<DBObject> {

    private static final byte TYPE_CHAR = Byte.MIN_VALUE;
    private static final byte TYPE_BYTE = Byte.MIN_VALUE + 1;
    private static final byte TYPE_STRING = Byte.MIN_VALUE + 2;
    private static final byte TYPE_BOOLEAN = Byte.MIN_VALUE + 3;
    private static final byte TYPE_INT = Byte.MIN_VALUE + 4;
    private static final byte TYPE_LONG = Byte.MIN_VALUE + 5;
    private static final byte TYPE_FLOAT = Byte.MIN_VALUE + 6;
    private static final byte TYPE_DOUBLE = Byte.MIN_VALUE + 7;
    private static final byte TYPE_SHORT = Byte.MIN_VALUE + 8;
    private static final byte TYPE_UUID = Byte.MIN_VALUE + 9;
    private static final byte TYPE_DATE = Byte.MIN_VALUE + 10;
    private static final byte TYPE_BIGINT = Byte.MIN_VALUE + 11;
    private static final byte TYPE_BIGDECIMAL = Byte.MIN_VALUE + 12;
    private static final byte TYPE_BYTEARRAY = Byte.MIN_VALUE + 13;
    private static final byte TYPE_ARRAY = Byte.MAX_VALUE - 1;
    private static final byte TYPE_COLLECTION = Byte.MAX_VALUE - 2;
    private static final byte TYPE_MAP = Byte.MAX_VALUE - 3;
    private static final byte TYPE_ENUM = Byte.MAX_VALUE - 4;
    private static final byte TYPE_OBJECTID = Byte.MAX_VALUE - 5;
    private static final byte TYPE_OBJECT = Byte.MAX_VALUE;

    private static final Map<Class<?>, Byte> typeCodes = new HashMap<Class<?>, Byte>();

    static {
        typeCodes.put(Boolean.class, TYPE_BOOLEAN);
        typeCodes.put(Byte.class, TYPE_BYTE);
        typeCodes.put(Character.class, TYPE_CHAR);
        typeCodes.put(Short.class, TYPE_SHORT);
        typeCodes.put(short.class, TYPE_SHORT);
        typeCodes.put(Integer.class, TYPE_INT);
        typeCodes.put(Long.class, TYPE_LONG);
        typeCodes.put(Float.class, TYPE_FLOAT);
        typeCodes.put(Double.class, TYPE_DOUBLE);
        typeCodes.put(String.class, TYPE_STRING);
        typeCodes.put(UUID.class, TYPE_UUID);
        typeCodes.put(Date.class, TYPE_DATE);
        typeCodes.put(BigInteger.class, TYPE_BIGINT);
        typeCodes.put(BigDecimal.class, TYPE_BIGDECIMAL);
        typeCodes.put(byte[].class, TYPE_BYTEARRAY);
        typeCodes.put(ObjectId.class, TYPE_OBJECTID);

    }

    private final PojoRepository repository = new PojoRepository();
    private final SpaceTypeDescriptor spaceTypeDescriptor;
    private final boolean prefferPojo;

    public DefaultSpaceDocumentMapper(SpaceTypeDescriptor spaceTypeDescriptor) {
        this(spaceTypeDescriptor, true);
    }

    public DefaultSpaceDocumentMapper(SpaceTypeDescriptor spaceTypeDescriptor, boolean prefferPojo) {
        this.spaceTypeDescriptor = spaceTypeDescriptor;
        this.prefferPojo = prefferPojo;
    }

    private byte type(Class c) {
        Byte type = typeCodes.get(c);
        if (type == null) {
            if (c.isEnum())
                type = TYPE_ENUM;
            else if (c.isArray())
                type = TYPE_ARRAY;
            else if (Collection.class.isAssignableFrom(c))
                type = TYPE_COLLECTION;
            else if (Map.class.isAssignableFrom(c))
                type = TYPE_MAP;
            else
                type = TYPE_OBJECT;
        }
        return type;
    }

    private byte bsonType(Object value) {

        Byte type = typeCodes.get(value.getClass());

        if (type == null) {

            if (BasicDBList.class.isInstance(value))
                type = TYPE_ARRAY;
            else
                type = TYPE_OBJECT;
        }

        return type;
    }

    public Object toDocument(DBObject bson) {

        if (bson == null)
            return null;

        String type = (String) bson.get(Constants.TYPE);

        if (prefferPojo && isPojo(type)) {
            return toPojo(bson);
        }

        return toSpaceDocument(bson);
    }

    private Object toPojo(DBObject bson) {

        String className = (String) bson.get(Constants.TYPE);

        try {
            Class<?> type = getClassFor(className);

            Object pojo = repository.getConstructor(getClassFor(className))
                    .newInstance();

            Iterator<String> iterator = bson.keySet().iterator();

            while (iterator.hasNext()) {

                String property = iterator.next();

                if (Constants.TYPE.equals(property))
                    continue;

                Object value = bson.get(property);

                boolean isArray = false;

                if (value instanceof BasicDBList) {
                    isArray = true;

                }

                if (value == null)
                    continue;

                if (Constants.ID_PROPERTY.equals(property))
                    property = spaceTypeDescriptor.getIdPropertyName();

                ISetterMethod<Object> setter = repository.getSetter(type,
                        property);

                Object val;
                if (isArray) {
                    val = toExtractArray((BasicDBList) value,
                            setter.getParameterTypes()[0]);
                } else {
                    val = fromDBObject(value);
                }

                if (type(setter.getParameterTypes()[0]) == TYPE_SHORT)
                    val = ((Integer) val).shortValue();

                setter.set(pojo, val);
            }
            return pojo;
        } catch (InvocationTargetException e) {
            throw new SpaceMongoException(
                    "can not invoke constructor or method: " + bson, e);
        } catch (IllegalAccessException e) {
            throw new SpaceMongoException(
                    "can not access constructor or method: " + bson, e);
        }
    }

    private Object toSpaceDocument(DBObject bson) {

        SpaceDocument document = new SpaceDocument((String) bson.get(Constants.TYPE));
        Iterator<String> iterator = bson.keySet().iterator();

        while (iterator.hasNext()) {

            String property = iterator.next();

            if (Constants.TYPE.equals(property))
                continue;

            Object value = bson.get(property);

            if (value == null)
                continue;

            if (Constants.ID_PROPERTY.equals(property))
                property = spaceTypeDescriptor.getIdPropertyName();

            document.setProperty(property, fromDBObject(value));
        }

        return document;
    }

    private boolean isPojo(String className) {
        try {
            Class.forName(className);
            return true;
        } catch (ClassNotFoundException e) {
            return false;
        }
    }

    public Object fromDBObject(Object value) {

        if (value == null)
            return null;

        switch (bsonType(value)) {
            case TYPE_OBJECTID:
                return null;
            case TYPE_ARRAY:
                return toExactArray((BasicDBList) value);
            case TYPE_OBJECT:
                return toExactObject(value);
            default:
                return value;
        }
    }

    @SuppressWarnings("unchecked")
    private Object toExactObject(Object value) {
        DBObject bson = (DBObject) value;

        if (bson.containsField(Constants.TYPE) && bson.containsField(Constants.VALUE)) {
            String t = (String) bson.get(Constants.TYPE);

            if (Constants.CUSTOM_BINARY.equals(t)) {
                Object result = deserializeObject(bson);

                return result;
            } else {
                try {
                    @SuppressWarnings("rawtypes")
                    Class type = Class.forName(t);

                    if (type.isEnum())
                        return Enum.valueOf(type, (String) bson.get(Constants.VALUE));
                    else
                        return fromSpecialType((DBObject) value);

                } catch (ClassNotFoundException e) {
                }
            }
        }

        return toDocument(bson);
    }

    private Object deserializeObject(DBObject bson) {
        Object result = null;
        try {
            ByteArrayInputStream bis = new ByteArrayInputStream(
                    (byte[]) bson.get(Constants.VALUE));

            ObjectInputStream in = new ObjectInputStream(bis);

            try {
                result = in.readObject();
            } finally {
                in.close();
                bis.close();
            }
        } catch (IOException e1) {
            throw new SpaceMongoException("can not deserialize object", e1);
        } catch (ClassNotFoundException e) {
            throw new SpaceMongoException("can not deserialize object", e);
        }

        return result;
    }

    private Object toExactArray(BasicDBList value) {

        if (value.size() < 1)
            throw new IllegalStateException("Illegal BSON array size: "
                    + value.size() + ", size must be at lest 1");

        Class<?> type = getClassFor((String) value.get(0));

        return toExtractArray(value, type);
    }

    private Object toExtractArray(BasicDBList value, Class<?> type) {
        if (type.isArray()) {
            return toArray(type, value);
        } else if (Collection.class.isAssignableFrom(type)) {
            return toCollection(type, value);
        } else if (Map.class.isAssignableFrom(type)) {
            return toMap(type, value);
        }

        throw new SpaceMongoException("invalid Array/Collection/Map type: "
                + type.getName());
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private Map toMap(Class<?> type, BasicDBList value) {

        Map map;

        if (!type.isInterface()) {
            map = (Map) repository.getConstructor(type).newInstance();
        } else {
            map = (Map) repository.getConstructor(
                    getClassFor((String) value.get(0))).newInstance();
        }
        for (int i = 1; i < value.size(); i += 2) {
            Object key = fromDBObject(value.get(i));
            Object val = fromDBObject(value.get(i + 1));

            map.put(key, val);
        }

        return map;

    }

    private Object toArray(Class<?> type, BasicDBList value) {

        int length = value.size() - 1;
        Object array = Array.newInstance(type.getComponentType(), length);

        for (int i = 1; i < length + 1; i++) {
            Object v = fromDBObject(value.get(i));

            if (SpaceDocument.class.isAssignableFrom(type.getComponentType()))
                v = MongoDocumentObjectConverter.instance().toDocumentIfNeeded(
                        v, SpaceDocumentSupport.CONVERT);

            if (type(type.getComponentType()) == TYPE_SHORT)
                v = ((Integer) v).shortValue();

            Array.set(array, i - 1, v);
        }

        return array;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private Collection toCollection(Class<?> type, BasicDBList value) {

        Collection collection;
        if (!type.isInterface()) {
            collection = (Collection) repository.getConstructor(type)
                    .newInstance();
        } else {
            collection = (Collection) repository.getConstructor(
                    getClassFor((String) value.get(0))).newInstance();
        }

        for (int i = 1; i < value.size(); i++)
            collection.add(fromDBObject(value.get(i)));

        return collection;
    }

    public Class<?> getClassFor(String type) {
        try {
            return Class.forName(type);
        } catch (ClassNotFoundException e) {
            throw new SpaceMongoException("Could not resolve type for type: "
                    + type, e);
        }
    }

    public DBObject toDBObject(Object document) {

        if (document == null)
            return null;

        if (document instanceof SpaceDocument)
            return toDBObjectDocument((SpaceDocument) document);

        return toDBObjectPojo(document);
    }

    private DBObject toDBObjectDocument(SpaceDocument document) {
        BasicDBObjectBuilder bson = BasicDBObjectBuilder.start();

        Set<String> keys = document.getProperties().keySet();

        bson.add(Constants.TYPE, document.getTypeName());

        for (String property : keys) {

            Object value = document.getProperty(property);

            if (value == null)
                continue;

            if (spaceTypeDescriptor.getIdPropertyName().equals(property))
                property = Constants.ID_PROPERTY;

            bson.add(property, toObject(value));
        }

        return bson.get();
    }

    private DBObject toDBObjectPojo(Object pojo) {

        BasicDBObjectBuilder bson = BasicDBObjectBuilder.start();

        Map<String, Method> getters = repository.getGetters(pojo.getClass());

        Class<?> type = pojo.getClass();

        bson.add(Constants.TYPE, type.getName());

        for (String property : getters.keySet()) {
            Object value = null;
            try {

                value = repository.getGetter(type, property).get(pojo);

                if (value == null)
                    continue;

                if (spaceTypeDescriptor.getIdPropertyName().equals(property))
                    property = Constants.ID_PROPERTY;

                bson.add(property, toObject(value));

            } catch (IllegalArgumentException e) {
                throw new SpaceMongoException("Argument is: " + value, e);
            } catch (IllegalAccessException e) {
                throw new SpaceMongoException("Can not access method", e);
            } catch (InvocationTargetException e) {
                throw new SpaceMongoException("Can not invoke method", e);
            }
        }

        return bson.get();
    }

    public Object toObject(Object property) {
        if (property == null)
            return null;

        switch (type(property.getClass())) {

            case TYPE_CHAR:
            case TYPE_FLOAT:
            case TYPE_BYTE:
            case TYPE_BIGDECIMAL:
            case TYPE_BIGINT:
                return toSpecialType(property);
            case TYPE_OBJECT:
                if (property instanceof SpaceDocument)
                    return toDBObject((SpaceDocument) property);
                else if (property instanceof Class)
                    return toSpecialType(property);
                else if (property instanceof Locale)
                    return toSpecialType(property);
                else if (property instanceof URI)
                    return toSpecialType(property);
                else if (property instanceof Timestamp)
                    return toSpecialType(property);

                if (!(property instanceof Serializable))
                    return toDBObject(property);

                byte[] result = serializeObject(property);

                BasicDBObjectBuilder blob = BasicDBObjectBuilder.start();

                blob.add(Constants.TYPE, Constants.CUSTOM_BINARY);

                blob.add(Constants.VALUE, result);

                blob.add(Constants.HASH, Arrays.hashCode(result));

                return blob.get();
            case TYPE_ENUM:
                return toEnum(property);
            case TYPE_ARRAY:
                return toArray(property);
            case TYPE_COLLECTION:
                return toCollection(property);
            case TYPE_MAP:
                return toMap(property);
            default:
                return property;
        }
    }

    private byte[] serializeObject(Object property) {

        byte[] result;

        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();

            ObjectOutputStream output = new ObjectOutputStream(bos);

            try {
                output.writeObject(property);

                result = bos.toByteArray();
            } finally {
                output.close();
                bos.close();
            }

        } catch (IOException e) {
            throw new SpaceMongoException("can not serialize object of class "
                    + property.getClass().getName());
        }

        return result;
    }

    private Object toEnum(Object property) {

        BasicDBObjectBuilder document = BasicDBObjectBuilder.start();

        return document.add(Constants.TYPE, property.getClass().getName())
                .add(Constants.VALUE, property.toString()).get();
    }

    private BasicDBList toMap(Object property) {

        BasicDBList builder = new BasicDBList();

        @SuppressWarnings("rawtypes")
        Map<?, ?> map = (Map) property;

        builder.add(property.getClass().getName());

        for (Map.Entry<?, ?> entry : map.entrySet()) {
            builder.add(toObject(entry.getKey()));
            builder.add(toObject(entry.getValue()));
        }

        return builder;
    }

    private BasicDBList toCollection(Object property) {

        BasicDBList builder = new BasicDBList();

        @SuppressWarnings("rawtypes")
        Collection collection = (Collection) property;

        builder.add(property.getClass().getName());

        for (Object e : collection) {
            builder.add(toObject(e));
        }

        return builder;
    }

    private BasicDBList toArray(Object property) {
        BasicDBList builder = new BasicDBList();

        int length = Array.getLength(property);

        builder.add(property.getClass().getName());

        for (int i = 0; i < length; i++) {
            Object obj = toObject(Array.get(property, i));
            setArray(builder, obj);
        }

        return builder;
    }

    private void setArray(BasicDBList builder, Object obj) {
        if (obj == null) {
            builder.add(null);
            return;
        }

        switch (type(obj.getClass())) {
            case TYPE_INT:
                builder.add(((Integer) obj).intValue());
                break;
            case TYPE_SHORT:
                //short is converted to int in mongo
                builder.add(((Short) obj).intValue());
                break;
            case TYPE_LONG:
                builder.add(((Long) obj).longValue());
                break;
            case TYPE_DOUBLE:
                builder.add(((Double) obj).doubleValue());
                break;
            default:
                builder.add(obj);
                break;
        }
    }

    private Character toCharacter(Object value) {
        if (value == null)
            return null;

        if (value instanceof String)
            return ((String) value).charAt(0);

        throw new IllegalArgumentException("invalid value for Character: "
                + value);
    }

    private Object fromSpecialType(DBObject value) {
        String type = (String) value.get(Constants.TYPE);
        String val = (String) value.get(Constants.VALUE);

        if (BigInteger.class.getName().equals(type))
            return new BigInteger(val);
        else if (BigDecimal.class.getName().equals(type))
            return new BigDecimal(val);
        else if (Byte.class.getName().equals(type))
            return Byte.valueOf(val);
        else if (Float.class.getName().equals(type))
            return Float.valueOf(val);
        else if (Character.class.getName().equals(type))
            return toCharacter(val);
        else if (Class.class.getName().equals(type))
            return toClass(val);
        else if (Locale.class.getName().equals(type))
            return toLocale(val);
        else if (URI.class.getName().equals(type))
            return URI.create(val);
        else if (Timestamp.class.getName().equals(type))
            return Timestamp.valueOf(val);

        throw new IllegalArgumentException("unkown value: " + value);
    }

    /**
     * Convert string representation to Locale object
     */
    private Locale toLocale(String str) {
        if (str == null)
            return null;

        String[] split = str.split("_");
        if (split.length == 0)
            return new Locale("");

        else if (split.length == 1)
            return new Locale(split[0]);

        else if (split.length == 2)
            return new Locale(split[0], split[1]);

            // ignore the rest - will be restored by the Locale constructor
        else
            return new Locale(split[0], split[1], split[2]);

    }

    private Class toClass(Object value) {
        if (value == null)
            return null;

        if (value instanceof String)
            try {
                return Class.forName((String) value);
            } catch (ClassNotFoundException e) {
                throw new IllegalArgumentException(e);
            }

        throw new IllegalArgumentException("invalid value for Character: "
                + value);
    }

    private DBObject toSpecialType(Object property) {
        BasicDBObjectBuilder document = BasicDBObjectBuilder.start();

        String toString = toString(property);

        return document.add(Constants.TYPE, property.getClass().getName())
                .add(Constants.VALUE, toString)
                .get();
    }

    private String toString(Object property) {
        if (property instanceof Class)
            return ((Class) property).getName();
        return property.toString();
    }
}
