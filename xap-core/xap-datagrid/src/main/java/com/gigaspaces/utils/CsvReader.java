package com.gigaspaces.utils;

import com.gigaspaces.document.SpaceDocument;
import com.gigaspaces.internal.metadata.*;
import com.gigaspaces.metadata.SpacePropertyDescriptor;
import com.gigaspaces.metadata.SpaceTypeDescriptor;
import com.gigaspaces.metadata.SpaceTypeDescriptorBuilder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * @author Niv Ingberg
 * @since 14.5
 */
public class CsvReader {

    private final String valuesSeparator;
    private final String metadataSeparator;
    private final Map<String, Function<String, Object>> parsers;

    public CsvReader() {
        this(new Builder());
    }

    private CsvReader(Builder builder) {
        this.valuesSeparator = builder.valuesSeparator;
        this.metadataSeparator = builder.metadataSeparator;
        this.parsers = builder.parsers;
    }

    public <T> Stream<T> read(Path path, Class<T> type) throws IOException {
        return new PojoProcessor<>(type).stream(path);
    }

    public Stream<SpaceDocument> read(Path path, String typeName) throws IOException {
        return read(path, new SpaceTypeDescriptorBuilder(typeName).create());
    }

    public Stream<SpaceDocument> read(Path path, SpaceTypeDescriptor typeDescriptor) throws IOException {
        return new DocumentProcessor(typeDescriptor).stream(path);
    }

    private abstract class Processor<T> {
        protected PropertyMapper[] properties;

        public Stream<T> stream(Path path) throws IOException {
            return Files.lines(path)
                    .map(line -> line.split(valuesSeparator, -1))
                    .filter(this::filterHeader)
                    .map(this::processEntry);
        }

        private boolean filterHeader(String[] values) {
            if (properties == null) {
                properties = new PropertyMapper[values.length];
                for (int i = 0; i < values.length; i++) {
                    String[] tokens = values[i].split(metadataSeparator);
                    properties[i] = initProperty(i, tokens[0], tokens.length != 1 ? tokens[1] : null);
                }
                return false;
            }
            return true;
        }

        private T processEntry(String[] values) {
            if (values.length != properties.length)
                throw new IllegalStateException(String.format("Inconsistent values: expected %s, actual %s", properties.length, values.length));
            return toEntry(values);
        }

        abstract PropertyMapper initProperty(int i, String name, String typeName);

        abstract T toEntry(String[] values);

        protected Function<String, Object> getParser(String typeName) {
            Function<String, Object> parser = parsers.get(typeName);
            if (parser == null)
                throw new IllegalArgumentException("No parser for type " + typeName);
            return parser;
        }
    }

    private class DocumentProcessor extends Processor<SpaceDocument> {
        private final SpaceTypeDescriptor typeDescriptor;

        private DocumentProcessor(SpaceTypeDescriptor typeDescriptor) {
            this.typeDescriptor = typeDescriptor;
        }

        @Override
        protected PropertyMapper initProperty(int i, String name, String typeName) {
            if (typeName == null) {
                SpacePropertyDescriptor property = typeDescriptor.getFixedProperty(name);
                if (property == null) {
                    throw new IllegalStateException("No metadata for property " + name);
                }
                typeName = property.getTypeName();
            }
            return new PropertyMapper(name, -1, getParser(typeName));
        }

        SpaceDocument toEntry(String[] values) {
            SpaceDocument result = new SpaceDocument(typeDescriptor.getTypeName());
            for (int i = 0; i < values.length; i++) {
                if (!values[i].isEmpty())
                    result.setProperty(properties[i].name, properties[i].parser.apply(values[i]));
            }
            return result;
        }
    }

    private class PojoProcessor<T> extends Processor<T> {
        private final SpaceTypeInfo typeInfo;

        private PojoProcessor(Class<T> type) {
            this.typeInfo = SpaceTypeInfoRepository.getTypeInfo(type);
        }

        @Override
        protected PropertyMapper initProperty(int i, String name, String typeName) {
            SpacePropertyInfo property = typeInfo.getProperty(name);
            if (property == null)
                throw new IllegalArgumentException("No such property: " + name);
            if (typeName == null)
                typeName = property.getTypeName();
            return new PropertyMapper(property.getName(), typeInfo.indexOf(property), getParser(typeName));
        }

        T toEntry(String[] values)  {
            Object[] allValues = new Object[typeInfo.getNumOfSpaceProperties()];
            for (int i = 0; i < values.length; i++) {
                if (!values[i].isEmpty())
                    allValues[properties[i].pos] = properties[i].parser.apply(values[i]);
            }
            Object result = typeInfo.createInstance();
            typeInfo.setSpacePropertiesValues(result, allValues);
            return (T) result;
        }
    }

    private static class PropertyMapper {
        private final String name;
        private final Function<String, Object> parser;
        private final int pos;

        private PropertyMapper(String name, int pos, Function<String, Object> parser) {
            this.name = name;
            this.pos = pos;
            this.parser = parser;
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String valuesSeparator = ",";
        private String metadataSeparator = ":";
        private final Map<String, Function<String, Object>> parsers = initDefaultParsers();

        public Builder valuesSeparator(String valuesSeparator) {
            this.valuesSeparator = valuesSeparator;
            return this;
        }

        public Builder metadataSeparator(String metadataSeparator) {
            this.metadataSeparator = metadataSeparator;
            return this;
        }

        public Builder addParser(String typeName, Function<String, Object> parser) {
            parsers.put(typeName, parser);
            return this;
        }

        public CsvReader build() {
            return new CsvReader(this);
        }

        private static Map<String, Function<String, Object>> initDefaultParsers() {
            Map<String, Function<String, Object>> result = new HashMap<>();

            result.put(String.class.getName(), s -> s);
            result.put("string", s -> s);
            result.put(boolean.class.getName(), Boolean::parseBoolean);
            result.put(Boolean.class.getName(), Boolean::parseBoolean);
            result.put(byte.class.getName(), Byte::parseByte);
            result.put(Byte.class.getName(), Byte::parseByte);
            result.put(short.class.getName(), Short::parseShort);
            result.put(Short.class.getName(), Short::parseShort);
            result.put(int.class.getName(), Integer::parseInt);
            result.put(Integer.class.getName(), Integer::parseInt);
            result.put(long.class.getName(), Long::parseLong);
            result.put(Long.class.getName(), Long::parseLong);
            result.put(float.class.getName(), Float::parseFloat);
            result.put(Float.class.getName(), Float::parseFloat);
            result.put(double.class.getName(), Double::parseDouble);
            result.put(Double.class.getName(), Double::parseDouble);
            result.put(char.class.getName(), s -> s.charAt(0));
            result.put(Character.class.getName(), s -> s.charAt(0));
            result.put(java.time.LocalDate.class.getName(), java.time.LocalDate::parse);
            result.put("date", java.time.LocalDate::parse);
            result.put(java.time.LocalTime.class.getName(), java.time.LocalTime::parse);
            result.put("time", java.time.LocalTime::parse);
            result.put(java.time.LocalDateTime.class.getName(), java.time.LocalDateTime::parse);
            result.put("datetime", java.time.LocalDateTime::parse);

            return result;
        }
    }
}
