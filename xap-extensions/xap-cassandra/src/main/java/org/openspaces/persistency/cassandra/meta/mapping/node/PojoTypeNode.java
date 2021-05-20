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

package org.openspaces.persistency.cassandra.meta.mapping.node;

import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.metadata.pojo.PojoPropertyInfo;
import com.gigaspaces.internal.metadata.pojo.PojoTypeInfo;
import com.gigaspaces.internal.metadata.pojo.PojoTypeInfoRepository;

import org.openspaces.persistency.cassandra.CassandraPersistencyConstants;
import org.openspaces.persistency.cassandra.error.SpaceCassandraTypeIntrospectionException;
import org.openspaces.persistency.cassandra.meta.DynamicColumnMetadata;
import org.openspaces.persistency.cassandra.meta.data.ColumnData;
import org.openspaces.persistency.cassandra.meta.data.ColumnFamilyRow;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

/**
 * POJO based {@link CompoundTypeNode}.
 *
 * @author Dan Kilman
 * @since 9.1.1
 */
public class PojoTypeNode extends AbstractCompoundTypeNode
        implements ExternalizableTypeNode {

    private static final long serialVersionUID = 1L;
    public static final byte SERIAL_VER = Byte.MIN_VALUE;

    // _name if this is a direct child of the top level type node
    // path from top level type node otherwise
    private String fullName;

    // property name
    private String name;

    // POJO type
    private Class<?> type;

    private Constructor<Object> constructor;

    private final Map<String, Method> getters = new HashMap<String, Method>();
    private final Map<String, Method> setters = new HashMap<String, Method>();

    /* for Externalizable */
    public PojoTypeNode() {

    }

    public PojoTypeNode(String parentFullName, String name, Class<?> type, TypeNodeContext context) {
        fullName = (parentFullName != null ? parentFullName + "." : "") + name;
        this.name = name;
        this.type = type;

        initFields(context);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Class<?> getType() {
        return type;
    }

    @Override
    public String getFullName() {
        return fullName;
    }

    @SuppressWarnings("unchecked")
    private void initFields(TypeNodeContext context) {
        PojoTypeInfo typeInfo = PojoTypeInfoRepository.getPojoTypeInfo(type);

        try {
            constructor = (Constructor<Object>) type.getConstructor();
        } catch (SecurityException e) {
            throw new SpaceCassandraTypeIntrospectionException("Could not find default constructor for type: "
                    + type.getName(), e);
        } catch (NoSuchMethodException e) {
            throw new SpaceCassandraTypeIntrospectionException("Could not find default constructor for type: "
                    + type.getName(), e);
        }

        for (PojoPropertyInfo property : typeInfo.getProperties().values()) {
            if ("class".equals(property.getName())) {
                continue;
            }

            if (property.getGetterMethod() == null ||
                    property.getSetterMethod() == null) {
                continue;
            }

            getters.put(property.getName(), property.getGetterMethod());
            setters.put(property.getName(), property.getSetterMethod());

        }

        // this means we got here from readExternal so all the fixed properties 
        // were already aquired from getChildren
        if (context == null) {
            return;
        }

        introspectFixedProperties(context);
    }

    private void introspectFixedProperties(TypeNodeContext context) {
        context.increaseNestingLevel();
        try {
            for (Map.Entry<String, Method> entry : getters.entrySet()) {
                String name = entry.getKey();
                Class<?> type = entry.getValue().getReturnType();
                TypeNode typeNode = context.getTypeNodeIntrospector().introspect(fullName,
                        name,
                        type,
                        context);
                if (typeNode != null) {
                    getChildren().put(name, typeNode);
                }
            }
        } finally {
            context.descreaseNestingLevel();
        }
    }

    @Override
    protected String getDynamicHeaderColumnName() {
        return CassandraPersistencyConstants.POJO_ENTRY_COLUMN_PREFIX + type.getName();
    }

    @Override
    protected void writeToColumnFamilyRowImpl(Object value, ColumnFamilyRow row, TypeNodeContext context) {
        boolean noPropertiesSet = true;

        for (Map.Entry<String, Method> entry : getters.entrySet()) {
            String propertyName = entry.getKey();
            Method propertyGetter = entry.getValue();
            Object propertyValue;
            try {
                propertyValue = context.getTypeNodeIntrospector()
                        .getProcedureCache()
                        .getterMethodFor(propertyGetter)
                        .get(value);
            } catch (IllegalArgumentException e) {
                throw new SpaceCassandraTypeIntrospectionException("Failed getting value for property " +
                        propertyName + " from property " +
                        fullName, e);
            } catch (IllegalAccessException e) {
                throw new SpaceCassandraTypeIntrospectionException("Failed getting value for property " +
                        propertyName + " from property " +
                        fullName, e);
            } catch (InvocationTargetException e) {
                throw new SpaceCassandraTypeIntrospectionException("Failed getting value for property " +
                        propertyName + " from property " +
                        fullName, e);
            }

            if (propertyValue == null) {
                continue;
            }

            noPropertiesSet = false;

            writePropertyToColumnFamilyRow(row, propertyName, propertyValue, context);
        }

        // if this is dynamic then the abstract part takes care of the header for us
        // we do this to ensure that we bring back an empty pojo on read
        if (!context.isDynamic() && noPropertiesSet) {
            row.addColumnData(new ColumnData(true /* stub */,
                    new DynamicColumnMetadata(fullName,
                            context.getTypeNodeIntrospector().getDynamicPropertyValueSerializer())));
        }
    }

    @Override
    public Object readFromColumnFamilyRow(ColumnFamilyRow row, TypeNodeContext context) {
        Map<String, Object> values = new HashMap<String, Object>();

        for (TypeNode typeNode : getChildren().values()) {
            Object value = typeNode.readFromColumnFamilyRow(row, context);
            if (value != null) {
                values.put(typeNode.getName(), value);
            }
        }

        // it is possible that someone wrote an empty pojo
        // so we test for the pojo column header (see write impl)
        if (values.isEmpty() && !row.getColumns().containsKey(fullName)) {
            return null;
        }

        Object newInstance = context.getTypeNodeIntrospector()
                .getProcedureCache()
                .constructorFor(constructor)
                .newInstance();

        for (Map.Entry<String, Object> entry : values.entrySet()) {
            Object value = entry.getValue();
            Method setterMethod = setters.get(entry.getKey());

            try {
                context.getTypeNodeIntrospector()
                        .getProcedureCache()
                        .setterMethodFor(setterMethod)
                        .set(newInstance, value);
            } catch (IllegalArgumentException e) {
                throw new SpaceCassandraTypeIntrospectionException("Failed setting property value for property: "
                        + entry.getKey() + " of " + fullName, e);
            } catch (IllegalAccessException e) {
                throw new SpaceCassandraTypeIntrospectionException("Failed setting property value for property: "
                        + entry.getKey() + " of " + fullName, e);
            } catch (InvocationTargetException e) {
                throw new SpaceCassandraTypeIntrospectionException("Failed setting property value for property: "
                        + entry.getKey() + " of " + fullName, e);
            }
        }
        return newInstance;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeByte(SERIAL_VER);
        IOUtils.writeString(out, name);
        IOUtils.writeString(out, fullName);
        IOUtils.writeString(out, type.getName());
        IOUtils.writeObject(out, getChildren());
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        in.readByte(); // SERIAL_VER (currently, unused)
        name = IOUtils.readString(in);
        fullName = IOUtils.readString(in);
        type = Class.forName(IOUtils.readString(in), false, Thread.currentThread().getContextClassLoader());
        Map<String, TypeNode> children = IOUtils.readObject(in);
        getChildren().putAll(children);
        initFields(null);
    }
}
