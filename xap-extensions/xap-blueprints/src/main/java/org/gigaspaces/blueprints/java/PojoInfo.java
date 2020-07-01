package org.gigaspaces.blueprints.java;

import com.github.mustachejava.util.DecoratedCollection;
import com.github.mustachejava.util.*;
import org.gigaspaces.blueprints.TemplateUtils;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class PojoInfo {
    private final String className;
    private final String packageName;
    private final boolean hasCompoundKey;
    private PojoInfo compoundKeyClass;
    private final Set<String> imports = new LinkedHashSet<>();
    private final Set<String> warnings = new LinkedHashSet<>();
    private final Set<String> annotations = new LinkedHashSet<>();
    private final List<PropertyInfo> properties = new ArrayList<>();

    public PojoInfo(String className, String packageName) {
        this.className = className;
        this.packageName = packageName;
        this.hasCompoundKey = false;
    }

    public PojoInfo(String className, String packageName, boolean hasCompoundKey) {
        this.className = className;
        this.packageName = packageName;
        this.hasCompoundKey = hasCompoundKey;
        if(hasCompoundKey) {
            createCompoudKeyClass(packageName);
        }
    }

    private void createCompoudKeyClass(String packageName) {
        this.compoundKeyClass = new PojoInfo("Key", packageName);
        this.addPropertyWithAutoGenerateFalseAndEmbedded("Key", "Key");
        this.addImport("com.gigaspaces.annotation.pojo.*");
        this.addImport("com.gigaspaces.config.CompoundIdBase");
        this.addImport("javax.persistence.*");
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

    public DecoratedCollection<PropertyInfo> getDecoratedProperties() {
        return new DecoratedCollection<>(properties);
    }

    public PropertyInfo addProperty(String name, Class<?> type) {
        return addPropertyImpl(name, type);
    }

    public boolean hasCompoundKey(){
        return hasCompoundKey;
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

    public PojoInfo addPropertyWithAutoGenerate(String name, Class<?> type) {
        PropertyInfo propertyInfo = addPropertyImpl(name, type);
        propertyInfo.annotations.add("@SpaceId(autoGenerate=true)");
        return this;
    }

    public PojoInfo addPropertyWithAutoGenerateFalseAndEmbedded(String name, String simpleTypeName) {
        PropertyInfo propertyInfo = addPropertyImpl(name, simpleTypeName);
        propertyInfo.annotations.add("@SpaceId(autoGenerate = false)");
        propertyInfo.annotations.add("@EmbeddedId");

        return this;
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

    private static String toCamelCase(String s) {
        if (s.toUpperCase().equals(s))
            return s.toLowerCase();
        return s.substring(0, 1).toLowerCase() + s.substring(1);
    }
}
