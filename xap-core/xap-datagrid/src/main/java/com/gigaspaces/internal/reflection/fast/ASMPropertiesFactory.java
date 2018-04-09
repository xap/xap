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

package com.gigaspaces.internal.reflection.fast;

import com.gigaspaces.internal.metadata.SpacePropertyInfo;
import com.gigaspaces.internal.metadata.SpaceTypeInfo;
import com.gigaspaces.internal.reflection.IProperties;
import com.gigaspaces.internal.reflection.standard.StandardProperties;

import org.objectweb.gs.asm.ClassWriter;
import org.objectweb.gs.asm.Label;
import org.objectweb.gs.asm.Opcodes;
import org.objectweb.gs.asm.Type;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

/**
 * Creates ASM created classes that represents that provides fast methods "reflection" mechanism.
 *
 * @author GuyK
 * @since 7.0
 */
@com.gigaspaces.api.InternalApi
public class ASMPropertiesFactory {
    private static final String CLASS_PROPERTIES_POSTFIX_NAME = "GigaspacesProperties";

    public static synchronized <T> IProperties<T> getProperties(SpaceTypeInfo typeInfo)
            throws SecurityException, NoSuchMethodException {
        final String className = ASMFactoryUtils.getCreateClassNamePrefix(typeInfo.getName()) +
                CLASS_PROPERTIES_POSTFIX_NAME;

        try {
            final ClassLoader targetClassLoader = ASMFactoryUtils.getClassTargetLoader(typeInfo.getType());

            ClassWriter cw = new ClassWriter(ClassWriter.COMPUTE_MAXS);
            cw.visit(Opcodes.V1_5, Opcodes.ACC_PUBLIC, className.replace('.', '/'), null,
                    StandardProperties.INTERNAL_NAME, null);

            final String ownerClassNameInternalName = Type.getInternalName(typeInfo.getType());

            createConstructor(cw);
            createGetter(cw, ownerClassNameInternalName, typeInfo.getSpaceProperties());
            createSetter(cw, ownerClassNameInternalName, typeInfo.getSpaceProperties());

            cw.visitEnd();

            byte[] b = cw.toByteArray();
            Class definedClass = ASMFactoryUtils.defineClass(targetClassLoader, className, b);

            Object[] constructorArgs = new Object[]{typeInfo.getSpaceProperties()};
            return (IProperties<T>) definedClass.getConstructor(SpacePropertyInfo[].class).newInstance(constructorArgs);
        } catch (Exception e) {
            NoSuchMethodException ex = new NoSuchMethodException("Failed generating ASM properties wrapper: " + e.toString());
            ex.initCause(e);
            throw ex;
        }
    }

    private static void createConstructor(ClassWriter cw) {
        MethodGenerator mv = MethodGenerator.newConstructor(cw, StandardProperties.CTOR_DESC);
        mv.start();
        mv.loadThis();
        mv.loadVariable(1);
        mv.invokeConstructor(StandardProperties.INTERNAL_NAME, StandardProperties.CTOR_DESC);
        mv.returnVoid();
    }

    private static void createGetter(ClassWriter cw, String ownerTypeName, SpacePropertyInfo[] properties) {

        MethodGenerator mv = MethodGenerator.newMethod(cw, IProperties.Helper.getterName(), IProperties.Helper.getterDesc(), IProperties.Helper.exceptions());

        final int VAR_OBJ = 1;      // Object obj;
        final int VAR_POJO = 2;     // MyPojo pojo;
        final int VAR_RESULT = 3;   // Object[] result;

        // MyPojo pojo = (MyPojo)obj;
        mv.castVariableIntoVariable(VAR_OBJ, ownerTypeName, VAR_POJO);

        // Object[] result = new Object[properties.length];
        mv.newArray("java/lang/Object", properties.length);
        mv.storeVariable(VAR_RESULT);

        for (int i = 0; i < properties.length; ++i) {

            Method getter = properties[i].getGetterMethod();
            // result[i] = ...
            mv.loadVariable(VAR_RESULT);
            mv.loadConstant(i);

            if (Modifier.isPrivate(getter.getModifiers())) {
                // Private requires reflection
                // ... = this.getValue(pojo, i);
                mv.loadThis();
                mv.loadVariable(VAR_POJO);
                mv.loadConstant(i);
                mv.invokeMethod(StandardProperties.INTERNAL_NAME, StandardProperties.GET_VALUE_NAME,
                        StandardProperties.GET_VALUE_DESC);
            } else {
                // ... = pojo.getXXX();
                mv.loadVariable(VAR_POJO);
                mv.invokeMethod(ownerTypeName, getter.getName(), Type.getMethodDescriptor(getter));
                mv.boxIfNeeded(getter.getReturnType());
            }

            mv.storeArrayItem();
        }

        // return result;
        mv.loadVariable(VAR_RESULT);
        mv.returnObject();
    }

    private static void createSetter(ClassWriter cw, String ownerTypeName, SpacePropertyInfo[] properties) {

        MethodGenerator mv = MethodGenerator.newMethod(cw, IProperties.Helper.setterName(), IProperties.Helper.setterDesc());

        final int VAR_OBJ = 1;      // Object obj;
        final int VAR_VALUES = 2;   // Object[] values;
        final int VAR_POJO = 3;     // MyPojo pojo;

        // MyPojo pojo = (MyPojo)obj;
        mv.castVariableIntoVariable(VAR_OBJ, ownerTypeName, VAR_POJO);

        for (int i = 0; i < properties.length; ++i) {

            Method setter = properties[i].getSetterMethod();
            // A setter might be null if the current property is immutable. In that case it is populated via the
            // constructor and the null setter is skipped.
            if (setter == null)
                continue;

            if (Modifier.isPrivate(setter.getModifiers())) {
                // Private requires reflection: this.setValue(pojo, values[i], i);
                mv.loadThis();
                mv.loadVariable(VAR_POJO);
                mv.loadArrayItemFromVariable(VAR_VALUES, i);
                mv.loadConstant(i);
                mv.invokeMethod(StandardProperties.INTERNAL_NAME, StandardProperties.SET_VALUE_NAME,
                        StandardProperties.SET_VALUE_DESC);
            } else if (!properties[i].isPrimitive())
                generateSetter(mv, ownerTypeName, setter, i, VAR_POJO, VAR_VALUES);
            else if (properties[i].hasNullValue())
                generateSetterWithNullValue(mv, ownerTypeName, setter, i, VAR_POJO, VAR_VALUES);
            else {
                // if (values[i] != null) { generateSetter(...) }
                mv.loadArrayItemFromVariable(VAR_VALUES, i);
                Label nullValueLabel = mv.jumpIfNull();
                generateSetter(mv, ownerTypeName, setter, i, VAR_POJO, VAR_VALUES);
                mv.endIf(nullValueLabel);

                // If property is primitive without null value and value is null - skip it.
            }
        }

        mv.returnVoid();
    }

    private static void generateSetter(MethodGenerator mv, String ownerTypeName, Method setter, int propertyId,
                                       int pojoVariableId, int valuesVariableId) {
        // pojo.setXXX(values[i]);
        mv.loadVariable(pojoVariableId);
        mv.loadArrayItemFromVariable(valuesVariableId, propertyId);
        mv.unboxIfNeeded(setter.getParameterTypes()[0]);
        mv.invokeMethod(ownerTypeName, setter.getName(), Type.getMethodDescriptor(setter));
        // If setter returns a value (e.g. fluent), pop it from the stack.
        if (setter.getReturnType() != void.class)
            mv.pop();
    }

    private static void generateSetterWithNullValue(MethodGenerator mv, String ownerTypeName, Method setter,
                                                    int propertyId, int pojoVariableId, int valuesVariableId) {
        // pojo.setXXX(this.convertFromNullIfNeeded(values[i], i));
        mv.loadVariable(pojoVariableId);
        mv.loadThis();
        mv.loadArrayItemFromVariable(valuesVariableId, propertyId);
        mv.loadConstant(propertyId);
        mv.invokeMethod(StandardProperties.INTERNAL_NAME, StandardProperties.FROM_NULL_VALUE_NAME,
                StandardProperties.FROM_NULL_VALUE_DESC);
        mv.unboxIfNeeded(setter.getParameterTypes()[0]);
        mv.invokeMethod(ownerTypeName, setter.getName(), Type.getMethodDescriptor(setter));
        // If setter returns a value (e.g. fluent), pop it from the stack.
        if (setter.getReturnType() != void.class)
            mv.pop();
    }
}