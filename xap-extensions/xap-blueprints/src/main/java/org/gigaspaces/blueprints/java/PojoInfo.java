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
package org.gigaspaces.blueprints.java;

import org.gigaspaces.blueprints.TemplateUtils;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class PojoInfo {
    private String className;
    private String packageName;
    private PojoInfo compoundKeyClass;
    private final Set<String> imports = new LinkedHashSet<>();
    private final Set<String> warnings = new LinkedHashSet<>();
    private final Set<String> annotations = new LinkedHashSet<>();
    private final List<PropertyInfo> properties = new ArrayList<>();

    public PojoInfo(String className, String packageName) {
        this.className = className;
        this.packageName = packageName;
    }

    public PojoInfo(String className, String packageName, boolean hasCompoundId) {
        this.className = className;
        this.packageName = packageName;
        if(hasCompoundId) {
            createCompoundKeyClass(packageName);
        }
    }

    private void createCompoundKeyClass(String packageName) {
        this.compoundKeyClass = new PojoInfo("Key", packageName);
        this.addCompoundKeyProperty("Key", "Key");
        this.addImport("com.gigaspaces.entry.CompoundSpaceId");
    }

    public String generate() throws IOException {
        return TemplateUtils.evaluateResource("templates/pojo.template", this);
    }

    public String getClassName() {
        return className;
    }

    public String getPackageName() {
        return packageName;
    }

    public Set<String> getImports() {
        return imports;
    }

    public Set<String> getWarnings() {
        return warnings;
    }

    public Set<String> getAnnotations() {
        return annotations;
    }

    public PojoInfo getCompoundKeyClass() {
        return compoundKeyClass;
    }

    public PojoInfo annotate(String annotation) {
        this.annotations.add(annotation);
        return this;
    }

    public PojoInfo addImport(String imported) {
        this.imports.add(imported);
        return this;
    }

    public PojoInfo addWarning(String warning) {
        this.warnings.add(warning);
        return this;
    }

    public List<PropertyInfo> getProperties() {
        return properties;
    }

    public PropertyInfo addProperty(String name, Class<?> type) {
        return addPropertyImpl(name, type);
    }

    public PropertyInfo addPropertyWithAnnotation(String name, Class<?> type, String annotation) {
        PropertyInfo propertyInfo = addPropertyImpl(name, type);
        propertyInfo.annotations.add(annotation);
        return propertyInfo;
    }

    public PropertyInfo addProperty(String name, String simpleTypeName) {
        return addPropertyImpl(name, simpleTypeName);
    }

    private PropertyInfo addPropertyImpl(String name, String simpleTypeName) {
        PropertyInfo propertyInfo = new PropertyInfo(name, simpleTypeName,properties.size());
        properties.add(propertyInfo);

        return propertyInfo;
    }

    private PropertyInfo addPropertyImpl(String name, Class<?> type) {
        PropertyInfo propertyInfo = new PropertyInfo(name, type, properties.size());
        properties.add(propertyInfo);

        Package typePackage = type.getPackage();
        if (typePackage != null && !typePackage.getName().equals("java.lang"))
            imports.add(typePackage.getName() + ".*");
        return propertyInfo;
    }

    public void addPropertyWithAutoGenerate(String name, Class<?> type) {
        PropertyInfo propertyInfo = addPropertyImpl(name, type);
        propertyInfo.annotations.add("@SpaceId(autoGenerate=true)");
    }

    public void addCompoundKeyProperty(String name, String simpleTypeName) {
        PropertyInfo propertyInfo = addPropertyImpl(name, simpleTypeName);
        propertyInfo.annotations.add("@SpaceId");
        propertyInfo.annotations.add("@EmbeddedId");
    }

    public String getPropertiesAsString(){
        return properties.stream().map( PropertyInfo::getFieldName )
                .collect( Collectors.joining( "," ) );
    }

    public String getPropertiesAsStringWithType(){
        return properties.stream().map( property -> property.simpleTypeName+" "+property.camelCaseName )
                .collect( Collectors.joining( ", " ) );
    }

    public static class PropertyInfo {
        private final String name;
        private final String camelCaseName;
        private final Class<?> type;
        private final String simpleTypeName;
        private final int index;
        private final Set<String> annotations = new LinkedHashSet<>();

        public PropertyInfo(String name, Class<?> type, int index) {
            this.name = name;
            this.camelCaseName = toCamelCase(name);
            this.type = type;
            this.simpleTypeName = type.getSimpleName();
            this.index = index;
        }

        public PropertyInfo(String name, String simpleTypeName, int index) {
            this.name = name;
            this.camelCaseName = toCamelCase(name);
            this.type = null;
            this.simpleTypeName = simpleTypeName;
            this.index = index;
        }

        public String getName() {
            return name;
        }

        public String getFieldName() {
            return camelCaseName;
        }

        public String getTypeName() {
            return simpleTypeName;
        }

        public int getIndex() {
            return index;
        }

        public Set<String> getAnnotations() {
            return annotations;
        }

        public PropertyInfo annotate(String annotation) {
            annotations.add(annotation);
            return this;
        }
    }

    public static String toCamelCase(String s) {
        if (s.toUpperCase().equals(s))
            return s.toLowerCase();
        return s.substring(0, 1).toLowerCase() + s.substring(1);
    }
}
