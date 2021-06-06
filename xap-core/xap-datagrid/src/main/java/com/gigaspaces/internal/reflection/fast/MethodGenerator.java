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

import org.objectweb.gs.asm.ClassVisitor;
import org.objectweb.gs.asm.Label;
import org.objectweb.gs.asm.MethodVisitor;
import org.objectweb.gs.asm.Opcodes;
import org.objectweb.gs.asm.Type;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

/**
 * @author Niv Ingberg
 * @since 9.6.1
 */
@com.gigaspaces.api.InternalApi
public class MethodGenerator {

    private final MethodVisitor mv;

    private MethodGenerator(ClassVisitor classVisitor, int access, String name, String desc, String signature,
                            String[] exceptions) {
        this.mv = classVisitor.visitMethod(access, name, desc, signature, exceptions);
    }

    public static MethodGenerator newConstructor(ClassVisitor classVisitor, String desc) {
        return new MethodGenerator(classVisitor, Opcodes.ACC_PUBLIC, "<init>", desc, null, null);
    }

    public static MethodGenerator newStaticConstructor(ClassVisitor classVisitor, String desc) {
        return new MethodGenerator(classVisitor, Opcodes.ACC_STATIC, "<clinit>", desc, null, null);
    }

    public static MethodGenerator newMethod(ClassVisitor classVisitor, String name, String desc) {
        return new MethodGenerator(classVisitor, Opcodes.ACC_PUBLIC, name, desc, null, null);
    }

    public static MethodGenerator newMethod(ClassVisitor classVisitor, String name, String desc, String[] exceptions) {
        return new MethodGenerator(classVisitor, Opcodes.ACC_PUBLIC, name, desc, null, exceptions);
    }

    public static MethodGenerator newVarargsMethod(ClassVisitor classVisitor, String name, String desc,
                                                   String[] exceptions) {
        return new MethodGenerator(classVisitor, Opcodes.ACC_PUBLIC | Opcodes.ACC_VARARGS, name, desc, null, exceptions);
    }


    ///////////////
    /// Returns ///
    ///////////////

    public void returnVoid() {
        returnResult(Opcodes.RETURN);
    }

    public void returnObject() {
        returnResult(Opcodes.ARETURN);
    }

    public void returnInt() {
        returnResult(Opcodes.IRETURN);
    }

    public void returnLong() {
        returnResult(Opcodes.LRETURN);
    }

    public void returnFloat() {
        returnResult(Opcodes.FRETURN);
    }

    public void returnDouble() {
        returnResult(Opcodes.DRETURN);
    }

    public void returnResult(Class clazz) {

        Type type = Type.getType(clazz);
        switch (type.getSort()) {
            case Type.VOID:
                returnVoid();
                break;
            case Type.BOOLEAN:
                checkCast("java/lang/Boolean");
                invokeMethod("java/lang/Boolean", "booleanValue", "()Z");
                returnInt();
                break;
            case Type.BYTE:
                checkCast("java/lang/Byte");
                invokeMethod("java/lang/Byte", "byteValue", "()B");
                returnInt();
                break;
            case Type.CHAR:
                checkCast("java/lang/Character");
                invokeMethod("java/lang/Character", "charValue", "()C");
                returnInt();
                break;
            case Type.SHORT:
                checkCast("java/lang/Short");
                invokeMethod("java/lang/Short", "shortValue", "()S");
                returnInt();
                break;
            case Type.INT:
                checkCast("java/lang/Integer");
                invokeMethod("java/lang/Integer", "intValue", "()I");
                returnInt();
                break;
            case Type.LONG:
                checkCast("java/lang/Long");
                invokeMethod("java/lang/Long", "longValue", "()J");
                returnLong();
                break;
            case Type.FLOAT:
                checkCast("java/lang/Float");
                invokeMethod("java/lang/Float", "floatValue", "()F");
                returnFloat();
                break;
            case Type.DOUBLE:
                checkCast("java/lang/Double");
                invokeMethod("java/lang/Double", "doubleValue", "()D");
                returnDouble();
                break;
            default:
                checkCast(type.getInternalName());
                returnObject();
                break;
        }
    }

    private void returnResult(int opcode) {
        mv.visitInsn(opcode);
        mv.visitMaxs(0, 0); // ignored by ClassWrite.COMPUTE_MAX
        mv.visitEnd();
    }

    /////////////////////
    /// Fields access ///
    /////////////////////

    public void loadField(Class owner, Field field) {
        int getOpCode = Modifier.isStatic(field.getModifiers()) ? Opcodes.GETSTATIC : Opcodes.GETFIELD;
        mv.visitFieldInsn(getOpCode, Type.getInternalName(owner), field.getName(),
                Type.getDescriptor(field.getType()));
    }

    public void storeField(Class owner, Field field) {
        int putOpCode = Modifier.isStatic(field.getModifiers()) ? Opcodes.PUTSTATIC : Opcodes.PUTFIELD;
        mv.visitFieldInsn(putOpCode, Type.getInternalName(owner), field.getName(),
                Type.getDescriptor(field.getType()));
    }

    public void loadField(String internalClassName, String name, String descriptor) {
        mv.visitFieldInsn(Opcodes.GETFIELD, internalClassName, name, descriptor);
    }

    public void storeField(String internalClassName, String name, String descriptor) {
        mv.visitFieldInsn(Opcodes.PUTFIELD, internalClassName, name, descriptor);
    }

    public void loadStaticField(String internalClassName, String name, String descriptor) {
        mv.visitFieldInsn(Opcodes.GETSTATIC, internalClassName, name, descriptor);
    }

    public void storeStaticField(String internalClassName, String name, String descriptor) {
        mv.visitFieldInsn(Opcodes.PUTSTATIC, internalClassName, name, descriptor);
    }

    ///////////////////////
    /// Variable access ///
    ///////////////////////

    public void loadVariable(int varIndex) {
        mv.visitVarInsn(Opcodes.ALOAD, varIndex);
    }

    public void loadThis() {
        mv.visitVarInsn(Opcodes.ALOAD, 0);
    }

    public void loadVariableInt(int varIndex) {
        mv.visitVarInsn(Opcodes.ILOAD, varIndex);
    }

    public void storeVariable(int varIndex) {
        mv.visitVarInsn(Opcodes.ASTORE, varIndex);
    }

    public int loadVariable(Class type, int index) {

        int sort = Type.getType(type).getSort();
        switch (sort) {
            case Type.BOOLEAN:
                mv.visitVarInsn(Opcodes.ILOAD, index);
                invokeStaticMethod(Boxer.INTERNAL_NAME, "box", "(Z)Ljava/lang/Object;");
                return 1;
            case Type.BYTE:
                mv.visitVarInsn(Opcodes.ILOAD, index);
                invokeStaticMethod(Boxer.INTERNAL_NAME, "box", "(B)Ljava/lang/Object;");
                return 1;
            case Type.CHAR:
                mv.visitVarInsn(Opcodes.ILOAD, index);
                invokeStaticMethod(Boxer.INTERNAL_NAME, "box", "(C)Ljava/lang/Object;");
                return 1;
            case Type.SHORT:
                mv.visitVarInsn(Opcodes.ILOAD, index);
                invokeStaticMethod(Boxer.INTERNAL_NAME, "box", "(S)Ljava/lang/Object;");
                return 1;
            case Type.INT:
                mv.visitVarInsn(Opcodes.ILOAD, index);
                invokeStaticMethod(Boxer.INTERNAL_NAME, "box", "(I)Ljava/lang/Object;");
                return 1;
            case Type.LONG:
                mv.visitVarInsn(Opcodes.LLOAD, index);
                invokeStaticMethod(Boxer.INTERNAL_NAME, "box", "(J)Ljava/lang/Object;");
                return 2;
            case Type.FLOAT:
                mv.visitVarInsn(Opcodes.FLOAD, index);
                invokeStaticMethod(Boxer.INTERNAL_NAME, "box", "(F)Ljava/lang/Object;");
                return 1;
            case Type.DOUBLE:
                mv.visitVarInsn(Opcodes.DLOAD, index);
                invokeStaticMethod(Boxer.INTERNAL_NAME, "box", "(D)Ljava/lang/Object;");
                return 2;
            default:
                loadVariable(index);
                return 1;
        }
    }

    /////////////////////
    /// Arrays access ///
    /////////////////////

    public void loadArrayItem() {
        mv.visitInsn(Opcodes.AALOAD);
    }

    public void loadArrayItemFromVariable(int variablePos, int index) {
        loadVariable(variablePos);
        loadConstant(index);
        loadArrayItem();
    }

    public void storeArrayItem() {
        mv.visitInsn(Opcodes.AASTORE);
    }

    //////////////////////////
    /// Method Invocations ///
    //////////////////////////

    public void invokeMethod(String owner, String name, String desc) {
        invokeMethodCustom(Opcodes.INVOKEVIRTUAL, owner, name, desc);
    }

    public void invokeStaticMethod(String owner, String name, String desc) {
        invokeMethodCustom(Opcodes.INVOKESTATIC, owner, name, desc);
    }

    public void invokeConstructor(String owner, String desc) {
        invokeMethodCustom(Opcodes.INVOKESPECIAL, owner, "<init>", desc);
    }

    public void invokeMethodCustom(int opcode, String owner, String name, String desc) {
        mv.visitMethodInsn(opcode, owner, name, desc);
    }

    ///////////////////////
    /// Type Operations ///
    ///////////////////////

    public void newInstance(String className) {
        mv.visitTypeInsn(Opcodes.NEW, className);
    }

    public void newArrayByVar(String className, int varIndex) {
        loadVariableInt(varIndex);
        mv.visitTypeInsn(Opcodes.ANEWARRAY, className);
    }

    public void newArray(String className, int length) {
        loadConstant(length);
        mv.visitTypeInsn(Opcodes.ANEWARRAY, className);
    }

    public void checkCast(String className) {
        mv.visitTypeInsn(Opcodes.CHECKCAST, className);
    }

    public void castVariable(int varIndex, String className) {
        loadVariable(varIndex);
        checkCast(className);
    }

    public void castVariableIntoVariable(int sourceVarIndex, String className, int targetVarIndex) {
        castVariable(sourceVarIndex, className);
        storeVariable(targetVarIndex);
    }

    ////////////////////
    /// Conditionals ///
    ////////////////////

    public Label jumpIfNull() {
        Label label = new Label();
        mv.visitJumpInsn(Opcodes.IFNULL, label);
        return label;
    }

    public void endIf(Label label) {
        mv.visitLabel(label);
    }

    //////////////////
    /// The Others ///
    //////////////////

    public void start() {
        mv.visitCode();
    }

    public void dup() {
        mv.visitInsn(Opcodes.DUP);
    }

    public void pop() {
        mv.visitInsn(Opcodes.POP);
    }

    public void loadConstant(Object value) {
        mv.visitLdcInsn(value);
    }

    ///////////////////////
    /// Boxing/Unboxing ///
    ///////////////////////

    public void unboxIfNeeded(Class clazz) {
        Type type = Type.getType(clazz);
        switch (Type.getType(clazz).getSort()) {
            case Type.BOOLEAN:
                checkCast("java/lang/Boolean");
                invokeMethod("java/lang/Boolean", "booleanValue", "()Z");
                break;
            case Type.BYTE:
                checkCast("java/lang/Byte");
                invokeMethod("java/lang/Byte", "byteValue", "()B");
                break;
            case Type.CHAR:
                checkCast("java/lang/Character");
                invokeMethod("java/lang/Character", "charValue", "()C");
                break;
            case Type.SHORT:
                checkCast("java/lang/Short");
                invokeMethod("java/lang/Short", "shortValue", "()S");
                break;
            case Type.INT:
                checkCast("java/lang/Integer");
                invokeMethod("java/lang/Integer", "intValue", "()I");
                break;
            case Type.LONG:
                checkCast("java/lang/Long");
                invokeMethod("java/lang/Long", "longValue", "()J");
                break;
            case Type.FLOAT:
                checkCast("java/lang/Float");
                invokeMethod("java/lang/Float", "floatValue", "()F");
                break;
            case Type.DOUBLE:
                checkCast("java/lang/Double");
                invokeMethod("java/lang/Double", "doubleValue", "()D");
                break;
            default:
                checkCast(type.getInternalName());
                break;
        }
    }

    public void boxIfNeeded(Class clazz) {
        if (!clazz.isPrimitive())
            return;

        int sort = Type.getType(clazz).getSort();
        switch (sort) {
            case Type.BOOLEAN:
                invokeStaticMethod("java/lang/Boolean", "valueOf", "(Z)Ljava/lang/Boolean;");
                break;
            case Type.BYTE:
                invokeStaticMethod("java/lang/Byte", "valueOf", "(B)Ljava/lang/Byte;");
                break;
            case Type.CHAR:
                invokeStaticMethod("java/lang/Character", "valueOf", "(C)Ljava/lang/Character;");
                break;
            case Type.SHORT:
                invokeStaticMethod("java/lang/Short", "valueOf", "(S)Ljava/lang/Short;");
                break;
            case Type.INT:
                invokeStaticMethod("java/lang/Integer", "valueOf", "(I)Ljava/lang/Integer;");
                break;
            case Type.LONG:
                invokeStaticMethod("java/lang/Long", "valueOf", "(J)Ljava/lang/Long;");
                break;
            case Type.FLOAT:
                invokeStaticMethod("java/lang/Float", "valueOf", "(F)Ljava/lang/Float;");
                break;
            case Type.DOUBLE:
                invokeStaticMethod("java/lang/Double", "valueOf", "(D)Ljava/lang/Double;");
                break;
        }
    }

    public void prepareResult(Class clazz) {
        Type type = Type.getType(clazz);
        switch (type.getSort()) {
            case Type.VOID:
                mv.visitInsn(Opcodes.ACONST_NULL); // nothing to return the original method returns void
                break;
            case Type.BOOLEAN:
                invokeStaticMethod(Boxer.INTERNAL_NAME, "box", "(Z)Ljava/lang/Object;");
                break;
            case Type.BYTE:
                invokeStaticMethod(Boxer.INTERNAL_NAME, "box", "(B)Ljava/lang/Object;");
                break;
            case Type.CHAR:
                invokeStaticMethod(Boxer.INTERNAL_NAME, "box", "(C)Ljava/lang/Object;");
                break;
            case Type.SHORT:
                invokeStaticMethod(Boxer.INTERNAL_NAME, "box", "(S)Ljava/lang/Object;");
                break;
            case Type.INT:
                invokeStaticMethod(Boxer.INTERNAL_NAME, "box", "(I)Ljava/lang/Object;");
                break;
            case Type.LONG:
                invokeStaticMethod(Boxer.INTERNAL_NAME, "box", "(J)Ljava/lang/Object;");
                break;
            case Type.FLOAT:
                invokeStaticMethod(Boxer.INTERNAL_NAME, "box", "(F)Ljava/lang/Object;");
                break;
            case Type.DOUBLE:
                invokeStaticMethod(Boxer.INTERNAL_NAME, "box", "(D)Ljava/lang/Object;");
                break;
        }
    }

}
