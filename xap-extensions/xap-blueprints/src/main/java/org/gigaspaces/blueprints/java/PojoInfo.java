package org.gigaspaces.blueprints.java;

import org.gigaspaces.blueprints.TemplateUtils;

import java.io.IOException;
import java.util.*;

public class PojoInfo {
    private final String className;
    private final String packageName;
    private final Set<String> imports = new LinkedHashSet<>();
    private final Set<String> warnings = new LinkedHashSet<>();
    private final Set<String> annotations = new LinkedHashSet<>();
    private final List<PropertyInfo> properties = new ArrayList<>();

    public PojoInfo(String className, String packageName) {
        this.className = className;
        this.packageName = packageName;
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

    public PojoInfo annotate(String annotation) {
        this.annotations.add(annotation);
        return this;
    }

    public List<PropertyInfo> getProperties() {
        return properties;
    }

    public PojoInfo addProperty(String name, Class<?> type) {
        properties.add(new PojoInfo.PropertyInfo(name, type));
        Package typePackage = type.getPackage();
        if (typePackage != null && !typePackage.getName().equals("java.lang"))
            imports.add(typePackage.getName() + ".*");
        return this;
    }

    public static class PropertyInfo {
        private final String name;
        private final String camelCaseName;
        private final Class<?> type;
        private final Set<String> annotations = new LinkedHashSet<>();

        public PropertyInfo(String name, Class<?> type) {
            this.name = name;
            this.camelCaseName = toCamelCase(name);
            this.type = type;
        }

        public String getName() {
            return name;
        }

        public String getFieldName() {
            return camelCaseName;
        }

        public String getTypeName() {
            return type.getSimpleName();
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
