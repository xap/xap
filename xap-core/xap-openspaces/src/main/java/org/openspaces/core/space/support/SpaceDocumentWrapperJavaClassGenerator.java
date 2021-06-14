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
package org.openspaces.core.space.support;

import com.gigaspaces.internal.dump.InternalDumpResult;
import com.gigaspaces.internal.utils.GsEnv;
import com.gigaspaces.metadata.SpaceTypeDescriptor;
import com.j_spaces.core.admin.IRemoteJSpaceAdmin;
import com.j_spaces.core.admin.SpaceRuntimeInfo;
import org.openspaces.core.GigaSpace;
import org.openspaces.core.GigaSpaceConfigurer;
import org.openspaces.core.space.SpaceProxyConfigurer;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * This class connects to a GigaSpace cluster and extracts all objects types and generates wrapper classes.
 * The wrapper class includes:
 * 1) A static method getTypeName() is added to return the real name of the SpaceDocument in the Space.
 * 2) A static final String is added for each field name to easily reference through the code (Foo_Wrapper.AGE < 8).
 * 3) Getters and return types matching the field value of the SpaceDocument property.
 *
 * Example:
 * ---
 * package com.gs.model;
 * import com.gigaspaces.document.SpaceDocument;
 * ...
 * public class Foo_Wrapper {
 *     private SpaceDocument document;
 *     public Foo_Wrapper(SpaceDocument document) {
 *         this.document = document;
 *     }
 *     public static String getTypeName() {
 *         return "com.mycompany.Foo"
 *     }
 *     public static final String NAME = "NAME";
 *     public String get_NAME() {
 *         return document.getProperty(NAME);
 *     }
 *
 *     public static final String AGE = "AGE";
 *     public Integer get_AGE() {
 *         return document.getProperty(AGE);
 *     }
 * }
 *
 * @since 16.0
 */
public class SpaceDocumentWrapperJavaClassGenerator {
    //delimiter between getter and field name - e.g. get_name()
    public String delimiter = "_";
    //generated class name suffix - e.g. Foo_Wrapper.java
    public String classnameSuffix = "_Wrapper";
    public String packageName = "com.gs.model";

    public void setTargetDir(String targetDir) {
        this.targetDir = targetDir;
    }

    public String targetDir = System.getProperty("java.io.tmpdir");

    public static void main(String[] args) throws Exception {
        final String spaceName = args[0];
        final String lookupGroups = GsEnv.property("com.gs.jini_lus.groups", "GS_LOOKUP_GROUPS").get();
        final String lookupLocators = GsEnv.property("com.gs.jini_lus.locators", "GS_LOOKUP_LOCATORS").get();

        final GigaSpace gigaSpace = new GigaSpaceConfigurer(
                new SpaceProxyConfigurer(spaceName)
                        .lookupTimeout(10000)
                        .lookupGroups(lookupGroups)
                        .lookupLocators(lookupLocators)).create();


        SpaceDocumentWrapperJavaClassGenerator generator = new SpaceDocumentWrapperJavaClassGenerator();
        generator.generate(gigaSpace, "java.lang.Object");
    }

    public void setDelimiter(String delimiter) {
        this.delimiter = delimiter;
    }

    public void setClassnameSuffix(String classnameSuffix) {
        this.classnameSuffix = classnameSuffix;
    }

    public void setPackageName(String packageName) {
        this.packageName = packageName;
    }

    /**
     * Generate the Java files of all the classes extending this className;
     * @param gigaSpace
     * @param className (e.g. java.lang.Object)
     * @throws Exception writing to file
     */
    public void generate(GigaSpace gigaSpace, String className) throws Exception {

        SpaceRuntimeInfo info;
        if (className == null)
         info = ((IRemoteJSpaceAdmin) gigaSpace.getSpace().getAdmin()).getRuntimeInfo();
        else
         info = ((IRemoteJSpaceAdmin) gigaSpace.getSpace().getAdmin()).getRuntimeInfo(className);

        for (String typeName : info.m_ClassNames) {
            if (typeName.equals("java.lang.Object")) continue; //skip

            StringBuilder header = new StringBuilder();
            StringBuilder body = new StringBuilder();
            //header
            header.append("package ").append(packageName).append(";").append("\n\n");
            header.append("import com.gigaspaces.document.SpaceDocument;").append("\n");

            //class body
            body.append("\n").append("public class ").append(getClassName(typeName)).append(" {").append("\n");

            //create wrapper field
            createField(body, "private SpaceDocument document;");

            //constructor
            createMethod(body, "", getClassName(typeName), "SpaceDocument document", "this.document = document;");

            //typeName
            createMethod(body, "static String", "getTypeName", "", "return \"" + typeName + "\";");

            //fields
            createGetters(gigaSpace, typeName, header, body);

            //close class definition
            body.append("}");

            writeToFiles(typeName, header, body);
        }
    }

    private void writeToFiles(String typeName, StringBuilder header, StringBuilder body) throws IOException {
        String path = targetDir + File.separator + getClassName(typeName) + ".java";
        File file = new File(path);
        if (file.exists()) {
            System.out.println("Overriding existing file: " + file.getAbsolutePath() + " (deleted="+file.delete()+")");
        } else {
            System.out.println("Writing to file: " + file.getAbsolutePath());
        }
        try (FileOutputStream out = new FileOutputStream(file)) {
            out.write(header.toString().getBytes());
            out.write(body.toString().getBytes());
        }
    }

    private void createGetters(GigaSpace gigaSpace, String typeName, StringBuilder header, StringBuilder body) {
        SpaceTypeDescriptor typeDescriptor = gigaSpace.getTypeManager().getTypeDescriptor(typeName);
        String[] propertiesNames = typeDescriptor.getPropertiesNames();
        String[] propertiesTypes = typeDescriptor.getPropertiesTypes();
        for (int i = 0; i < propertiesNames.length; i++) {
            String name = propertiesNames[i];
            String type = propertiesTypes[i];
            //add imports if necessary
            if (type.contains(".")
                    && !type.startsWith("java.")
                    && !header.toString().contains(type)) {
                header.append("import ").append(type).append(";\n");
            }
            //create constant field name
            createField(body, "public static final String " + name + " = \"" + name + "\";");

            //create getter method for field
            createMethod(body, type, "get" + delimiter + name, "", "return document.getProperty(" + name + ");");
        }
    }

    private String getClassName(String typeName) {
        String className = typeName;
        //if typeName includes also package, extract only type name
        if (typeName.contains(".")) {
            className = typeName.substring(typeName.lastIndexOf('.') + 1);
        }
        return className + classnameSuffix;
    }

    private void createMethod(StringBuilder builder, String returnType, String methodName, String args, String body) {
        builder.append("\t").append("public ").append(returnType).append(" ").append(methodName).append("(").append(args).append(") {").append("\n");
        builder.append("\t\t").append(body).append("\n");
        builder.append("\t").append("}").append("\n\n");
    }

    public void createField(StringBuilder builder, String field) {
        builder.append("\t").append(field).append("\n\n");
    }
}