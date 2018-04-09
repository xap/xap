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

import com.gigaspaces.internal.reflection.IProperties;

import org.objectweb.gs.asm.ClassWriter;
import org.objectweb.gs.asm.Opcodes;
import org.objectweb.gs.asm.Type;

import java.lang.reflect.Field;

/**
 * Creates ASM created classes that represents that provides fast fields "reflection" mechanism.
 *
 * @author GuyK
 * @since 7.0
 */
@com.gigaspaces.api.InternalApi
public class ASMFieldPropertiesFactory {

    final private static String CLASS_PROPERTIES_POSTFIX_NAME = "__GigaspacesFieldProperties__";

    public static <T> IProperties<T> getProperties(Class declaringClass, Field[] fields)
            throws NoSuchFieldException {
        String ownerClassName = declaringClass.getName();

        String className = ASMFactoryUtils.getCreateClassNamePrefix(ownerClassName) + CLASS_PROPERTIES_POSTFIX_NAME;

        try {
            final ClassLoader targetClassLoader = ASMFactoryUtils.getClassTargetLoader(declaringClass);
            String classInternalName = className.replace('.', '/'); // build internal name for ASM

            ClassWriter cw = new ClassWriter(ClassWriter.COMPUTE_MAXS);

            cw.visit(Opcodes.V1_5, Opcodes.ACC_PUBLIC,
                    classInternalName, null, "java/lang/Object", new String[]{IProperties.Helper.INTERNAL_NAME});

            createCtor(cw);

            String ownerClassNameInternalName = ownerClassName.replace('.', '/'); // build internal name for ASM
            createGetter(cw, ownerClassNameInternalName, fields);
            createSetter(cw, ownerClassNameInternalName, fields);

            cw.visitEnd();

            byte[] b = cw.toByteArray();
            Class<?> definedClass = ASMFactoryUtils.defineClass(targetClassLoader, className, b);

            return (IProperties<T>) definedClass.newInstance();
        } catch (Exception e) {
            NoSuchFieldException ex = new NoSuchFieldException("Can't create helper to: " + ownerClassName);
            ex.initCause(e);
            throw ex;
        }

    }

    /**
     * Creates the class constructor which delegates the input to the super.
     */
    private static void createCtor(ClassWriter cw) {
        MethodGenerator mv = MethodGenerator.newConstructor(cw, "()V");
        mv.start();
        mv.loadThis();
        mv.invokeConstructor("java/lang/Object", "()V");
        mv.returnVoid();
    }

    //TODO handle private methods
    private static void createGetter(ClassWriter cw, String internalClassName, Field[] fields) {

        MethodGenerator mv = MethodGenerator.newMethod(cw, IProperties.Helper.getterName(), IProperties.Helper.getterDesc(), IProperties.Helper.exceptions());

		/* MyPojo pojo = (MyPojo)obj; */
        mv.castVariableIntoVariable(1, internalClassName, 2);

		/* Object[] result = new Object[fields.length]; */
        mv.newArray("java/lang/Object", fields.length);
        mv.storeVariable(3);

        for (int i = 0; i < fields.length; ++i) {

            mv.loadVariable(3); // result
            mv.loadConstant(i); // [i]
            mv.loadVariable(2); // pojo

            Type type = Type.getType(fields[i].getType());
            mv.loadField(internalClassName, fields[i].getName(), type.getDescriptor());
            mv.storeArrayItem();
        }

		/* return result; */
        mv.loadVariable(3);
        mv.returnObject();
    }

    private static void createSetter(ClassWriter cw, String internalClassName, Field[] fields) {

        MethodGenerator mv = MethodGenerator.newMethod(cw, IProperties.Helper.setterName(), IProperties.Helper.setterDesc());

		/* MyPojo pojo = (MyPojo)obj; */
        mv.castVariableIntoVariable(1, internalClassName, 3);

        for (int i = 0; i < fields.length; ++i) {

            mv.loadVariable(3); // pojo
            mv.loadArrayItemFromVariable(2, i); // values[i]

            // cast to the setter type
            Type type = Type.getType(fields[i].getType());
            mv.checkCast(type.getInternalName());
            mv.storeField(internalClassName, fields[i].getName(), type.getDescriptor());
        }

        mv.returnVoid();
    }
}
