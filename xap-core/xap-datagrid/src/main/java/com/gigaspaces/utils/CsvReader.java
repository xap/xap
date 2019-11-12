package com.gigaspaces.utils;

import com.gigaspaces.document.SpaceDocument;
import com.gigaspaces.internal.metadata.SpacePropertyInfo;
import com.gigaspaces.internal.metadata.SpaceTypeInfo;
import com.gigaspaces.internal.metadata.SpaceTypeInfoRepository;
import com.gigaspaces.metadata.SpacePropertyDescriptor;
import com.gigaspaces.metadata.SpaceTypeDescriptor;
import com.gigaspaces.metadata.SpaceTypeDescriptorBuilder;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author Niv Ingberg
 * @since 14.5
 */

public class CsvReader {

    private final Charset charset;
    private final String valuesSeparator;
    private final String metadataSeparator;
    private final Map<String, Parser> parsers;
    private final Function<Line, Optional<String[]>> invalidLineParser;
    private final boolean removeQuotes;

    public CsvReader() {
        this(new Builder());
    }

    private CsvReader(Builder builder) {
        this.charset = builder.charset;
        this.valuesSeparator = builder.valuesSeparator;
        this.metadataSeparator = builder.metadataSeparator;
        this.parsers = builder.parsers;
        this.invalidLineParser = builder.invalidLineParser;
        this.removeQuotes = builder.removeQuotes;
    }

    private String unquote(String s) {
        if (s.startsWith("\"") && s.endsWith("\"") && removeQuotes)
            s = s.substring(1, s.length() - 1);
        return s;
    }

    public static Builder builder() {
        return new Builder();
    }

    public <T> Stream<T> read(Path path, Class<T> type) throws IOException {
        return new PojoProcessor<>(type).stream(path);
    }

    public <T> Stream<T> read(InputStream input, Class<T> type) throws IOException {
        return new PojoProcessor<>(type).stream(input);
    }

    public Stream<SpaceDocument> read(Path path, String typeName) throws IOException {
        return read(path, new SpaceTypeDescriptorBuilder(typeName).create());
    }

    public Stream<SpaceDocument> read(InputStream input, String typeName) throws IOException {
        return read(input, new SpaceTypeDescriptorBuilder(typeName).create());
    }

    public Stream<SpaceDocument> read(Path path, SpaceTypeDescriptor typeDescriptor) throws IOException {
        return new DocumentProcessor(typeDescriptor).stream(path);
    }

    public Stream<SpaceDocument> read(InputStream input, SpaceTypeDescriptor typeDescriptor) throws IOException {
        return new DocumentProcessor(typeDescriptor).stream(input);
    }

    public SpaceTypeDescriptorBuilder readSchema(Path path, String typeName) throws IOException {
        return new DocumentProcessor(new SpaceTypeDescriptorBuilder("dummy").create())
                .readSchema(path, typeName);
    }

    protected Parser getParser(String typeName) {
        Parser parser = parsers.get(typeName);
        if (parser == null)
            throw new IllegalArgumentException("No parser for type " + typeName);
        return parser;
    }

    private static class Parser {
        private final Class<?> type;
        private final Function<String, Object> parser;

        private Parser(Class<?> type, Function<String, Object> parser) {
            this.type = type;
            this.parser = parser;
        }
    }

    public static class Builder {
        private final Map<String, Parser> parsers = initDefaultParsers();
        private Charset charset = StandardCharsets.UTF_8;
        private String valuesSeparator = ",";
        private String metadataSeparator = ":";
        private boolean removeQuotes = true;
        private Function<Line, Optional<String[]>> invalidLineParser = line -> {
            throw new IllegalStateException(String.format("Inconsistent values at line #%s: expected %s, actual %s", line.getNum(), line.getNumOfColumns(), line.getNumOfValues()));
        };

        private static Map<String, Parser> initDefaultParsers() {
            Map<String, Parser> result = new HashMap<>();

            result.put(String.class.getName(), new Parser(String.class, s -> s));
            result.put(boolean.class.getName(), new Parser(boolean.class, Boolean::parseBoolean));
            result.put(Boolean.class.getName(), new Parser(Boolean.class, Boolean::parseBoolean));
            result.put(byte.class.getName(), new Parser(byte.class, Byte::parseByte));
            result.put(Byte.class.getName(), new Parser(Byte.class, Byte::parseByte));
            result.put(short.class.getName(), new Parser(short.class, Short::parseShort));
            result.put(Short.class.getName(), new Parser(Short.class, Short::parseShort));
            result.put(int.class.getName(), new Parser(int.class, Integer::parseInt));
            result.put(Integer.class.getName(), new Parser(Integer.class, Integer::parseInt));
            result.put(long.class.getName(), new Parser(long.class, Long::parseLong));
            result.put(Long.class.getName(), new Parser(Long.class, Long::parseLong));
            result.put(float.class.getName(), new Parser(float.class, Float::parseFloat));
            result.put(Float.class.getName(), new Parser(Float.class, Float::parseFloat));
            result.put(double.class.getName(), new Parser(double.class, Double::parseDouble));
            result.put(Double.class.getName(), new Parser(Double.class, Double::parseDouble));
            result.put(char.class.getName(), new Parser(char.class, s -> s.charAt(0)));
            result.put(Character.class.getName(), new Parser(Character.class, s -> s.charAt(0)));
            result.put(java.time.LocalDate.class.getName(), new Parser(java.time.LocalDate.class, java.time.LocalDate::parse));
            result.put(java.time.LocalTime.class.getName(), new Parser(java.time.LocalTime.class, java.time.LocalTime::parse));
            result.put(java.time.LocalDateTime.class.getName(), new Parser(java.time.LocalDateTime.class, java.time.LocalDateTime::parse));
            result.put("string", result.get(String.class.getName()));
            result.put("date", result.get(java.time.LocalDate.class.getName()));
            result.put("time", result.get(java.time.LocalTime.class.getName()));
            result.put("datetime", result.get(java.time.LocalDateTime.class.getName()));

            return result;
        }

        public Builder charset(Charset charset) {
            this.charset = charset;
            return this;
        }

        public Builder valuesSeparator(String valuesSeparator) {
            this.valuesSeparator = valuesSeparator;
            return this;
        }

        public Builder metadataSeparator(String metadataSeparator) {
            this.metadataSeparator = metadataSeparator;
            return this;
        }

        public Builder skipInvalidLines() {
            this.invalidLineParser = (l -> Optional.empty());
            return this;
        }

        public Builder invalidLineParser(Function<Line, Optional<String[]>> invalidLineParser) {
            this.invalidLineParser = invalidLineParser;
            return this;
        }

        public Builder addParser(String typeName, Class<?> type, Function<String, Object> parser) {
            parsers.put(typeName, new Parser(type, parser));
            return this;
        }

        public Builder removeQuotes(boolean removeQuotes){
            this.removeQuotes = removeQuotes;
            return this;
        }

        public CsvReader build() {
            return new CsvReader(this);
        }
    }

    private abstract class Processor<T> {
        protected List<Column> columns;
        private final AtomicLong counter = new AtomicLong();

        public Stream<T> stream(Path path) throws IOException {
            Stream<String> lines = Files.lines(path, charset);
            return stream(lines);
        }

        public Stream<T> stream(InputStream input) {
            return stream(wrap(input));
        }

        private Stream<String> wrap(InputStream inputStream) {
            // Copied from Files.lines(path)
            BufferedReader br = new BufferedReader(new InputStreamReader(inputStream, charset.newDecoder()));
            try {
                return br.lines().onClose(() -> close(br));
            } catch (Error|RuntimeException e) {
                try {
                    br.close();
                } catch (IOException ex) {
                    try {
                        e.addSuppressed(ex);
                    } catch (Throwable ignore) {}
                }
                throw e;
            }
        }

        private void close(Closeable resource) {
            try {
                resource.close();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        private Stream<T> stream(Stream<String> lines) {
            return lines.map(this::parseLine)
                    .filter(Objects::nonNull)
                    .map(this::toEntry);
        }

        public SpaceTypeDescriptorBuilder readSchema(Path path, String typeName) throws IOException {
            try (Stream<T> stream = stream(path)) {
                stream.findFirst();
            }
            SpaceTypeDescriptorBuilder typeDescriptorBuilder = new SpaceTypeDescriptorBuilder(typeName);
            for (Column column : columns)
                typeDescriptorBuilder.addFixedProperty(column.name, column.parser.type);
            return typeDescriptorBuilder;
        }

        private LineImpl parseLine(String text) {
            long id = counter.incrementAndGet();
            String[] values = text.split(valuesSeparator, -1);
            if (columns == null) {
                columns = Arrays.stream(values).map(this::toColumn).collect(Collectors.toList());
                return null;
            } else {
                LineImpl line = new LineImpl(id, text, values, columns);
                if (columns.size() != values.length) {
                    line = invalidLineParser.apply(line)
                            .map(newValues -> new LineImpl(id, text, newValues, columns))
                            .orElse(null);
                }
                return line;
            }
        }

        private Column toColumn(String s) {
            String[] tokens = unquote(s).split(metadataSeparator);
            return initColumn(tokens[0], tokens.length != 1 ? tokens[1] : null);
        }

        abstract Column initColumn(String name, String typeName);

        abstract T toEntry(LineImpl line);
    }

    private class DocumentProcessor extends Processor<SpaceDocument> {
        private final SpaceTypeDescriptor typeDescriptor;

        private DocumentProcessor(SpaceTypeDescriptor typeDescriptor) {
            this.typeDescriptor = typeDescriptor;
        }

        @Override
        protected Column initColumn(String name, String typeName) {
            SpacePropertyDescriptor property = getProperty(name);
            if (property != null)
                return new Column(property.getName(), typeName != null ? typeName : property.getTypeName(), -1);
            if (typeName != null)
                return new Column(name, typeName, -1);
            throw new IllegalStateException("No metadata for property " + name);
        }

        private SpacePropertyDescriptor getProperty(String name) {
            SpacePropertyDescriptor property = typeDescriptor.getFixedProperty(name);
            if (property != null)
                return property;
            for (int i=0 ; i < typeDescriptor.getNumOfFixedProperties() ; i++) {
                property = typeDescriptor.getFixedProperty(i);
                if (property.getName().equalsIgnoreCase(name))
                    return property;
            }
            return null;
        }

        SpaceDocument toEntry(LineImpl line) {
            SpaceDocument result = new SpaceDocument(typeDescriptor.getTypeName());
            String[] values = line.values;
            for (int i = 0; i < values.length; i++) {
                if (!values[i].isEmpty()) {
                    Column column = columns.get(i);
                    result.setProperty(column.name, column.parser.parser.apply(unquote(values[i])));
                }
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
        protected Column initColumn(String name, String typeName) {
            SpacePropertyInfo property = getProperty(name);
            return new Column(property.getName(), typeName != null ? typeName : property.getTypeName(), typeInfo.indexOf(property));
        }

        private SpacePropertyInfo getProperty(String name) {
            SpacePropertyInfo property = typeInfo.getProperty(name);
            if (property != null)
                return property;
            for (SpacePropertyInfo currProperty : typeInfo.getSpaceProperties())
                if (currProperty.getName().equalsIgnoreCase(name))
                    return currProperty;

            throw new IllegalArgumentException("No such property: " + name);
        }

        T toEntry(LineImpl line) {
            String[] values = line.values;
            Object[] allValues = new Object[typeInfo.getNumOfSpaceProperties()];
            for (int i = 0; i < values.length; i++) {
                if (!values[i].isEmpty()) {
                    Column column = columns.get(i);
                    allValues[column.pos] = column.parser.parser.apply(unquote(values[i]));
                }
            }
            Object result = typeInfo.createInstance();
            typeInfo.setSpacePropertiesValues(result, allValues);
            return (T) result;
        }
    }

    public interface Line {
        long getNum();
        String getText();
        int getNumOfValues();
        int getNumOfColumns();
    }

    private static class LineImpl implements Line {

        private final long num;
        private final String text;
        private final String[] values;
        private final List<Column> columns;

        private LineImpl(long num, String text, String[] values, List<Column> columns) {
            this.num = num;
            this.text = text;
            this.values = values;
            this.columns = columns;
        }

        @Override
        public long getNum() {
            return num;
        }

        @Override
        public String getText() {
            return text;
        }

        @Override
        public int getNumOfValues() {
            return values.length;
        }

        @Override
        public int getNumOfColumns() {
            return columns.size();
        }
    }

    private class Column {
        private final String name;
        private final Parser parser;
        private final int pos;

        private Column(String name, String typeName, int pos) {
            this.name = name;
            this.parser = getParser(typeName);
            this.pos = pos;
        }
    }
}
