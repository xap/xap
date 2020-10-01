package com.gigaspaces.utils;

import com.gigaspaces.document.SpaceDocument;
import com.gigaspaces.metadata.SpaceTypeDescriptor;
import com.gigaspaces.metadata.SpaceTypeDescriptorBuilder;
import org.hamcrest.core.StringContains;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CsvReaderTests {
    @Test
    public void testInvalidPath() {
        Path path = Paths.get("foo.csv");
        try {
            new CsvReader().read(path, "foo");
            Assert.fail("Should have failed - no such file");
        } catch (IOException e) {
            Assert.assertThat(e.getMessage(), new StringContains(path.toString()));
        }
    }

    @Test
    public void testCsvWithType() throws IOException {
        Path path = getResourcePath("csv/person.csv");
        List<Person> result = toList(new CsvReader().read(path, Person.class));
        assertPersonPojos(result);
    }

    @Test
    public void testCsvWithTypeName() throws IOException {
        String typeName = "person";
        Path path = getResourcePath("csv/person-with-types.csv");
        List<SpaceDocument> result = toList(new CsvReader().read(path, typeName));
        assertPersonDocuments(result, typeName);

        SpaceTypeDescriptor typeDescriptor = new CsvReader().readSchema(path, typeName).create();
        Assert.assertEquals(typeName, typeDescriptor.getTypeName());
        Assert.assertEquals(5, typeDescriptor.getNumOfFixedProperties());
        Assert.assertEquals(int.class, typeDescriptor.getFixedProperty("id").getType());
        Assert.assertEquals(String.class, typeDescriptor.getFixedProperty("name").getType());
        Assert.assertEquals(LocalDate.class, typeDescriptor.getFixedProperty("birthday").getType());
        Assert.assertEquals(boolean.class, typeDescriptor.getFixedProperty("native").getType());
        Assert.assertEquals(String.class, typeDescriptor.getFixedProperty("_spaceId").getType());
    }

    @Test
    public void testCsvWithTypeDescriptor() throws IOException {
        String typeName = "person";
        SpaceTypeDescriptor typeDescriptor = new SpaceTypeDescriptorBuilder(typeName)
                .addFixedProperty("id", Integer.class)
                .addFixedProperty("name", String.class)
                .addFixedProperty("birthday", LocalDate.class)
                .addFixedProperty("native", Boolean.class)
                .create();
        Path path = getResourcePath("csv/person.csv");
        List<SpaceDocument> result = toList(new CsvReader().read(path, typeDescriptor));
        assertPersonDocuments(result, typeName);
    }

    @Test
    public void testCsvWithoutMetadata() throws IOException {
        String typeName = "person";
        Path path = getResourcePath("csv/person.csv");
        try {
            toList(new CsvReader().read(path, typeName));
            Assert.fail("Should have failed - no metadata");
        } catch (IllegalStateException e) {
            Assert.assertEquals("No metadata for property ID", e.getMessage());
        }
    }

    @Test
    public void testCsvInconsistentValues() throws IOException {
        Path path = getResourcePath("csv/person-inconsistent-values.csv");
        try {
            toList(new CsvReader().read(path, Person.class));
            Assert.fail("Should have failed - inconsistent values");
        } catch (IllegalStateException e) {
            Assert.assertEquals("Inconsistent values at line #3: expected 4, actual 3", e.getMessage());
        }
    }

    @Test
    public void testCsvInconsistentValuesSkip() throws IOException {
        Path path = getResourcePath("csv/person-inconsistent-values.csv");
        List<Person> people = toList(CsvReader.builder()
                .skipInvalidLines()
                .build()
                .read(path, Person.class));
        Assert.assertEquals(1, people.size());
        Assert.assertEquals(1, people.get(0).getId());
        Assert.assertEquals("john", people.get(0).getName());
    }

    @Test
    public void testCsvInconsistentValuesCustom() throws IOException {
        Path path = getResourcePath("csv/person-inconsistent-values.csv");
        List<Person> people = toList(CsvReader.builder()
                .invalidLineParser(l -> Optional.of((l.getText() + ",false").split(",", -1)))
                .build()
                .read(path, Person.class));
        Assert.assertEquals(2, people.size());
        Assert.assertEquals(1, people.get(0).getId());
        Assert.assertEquals("john", people.get(0).getName());
        Assert.assertEquals(2, people.get(1).getId());
        Assert.assertEquals("jane", people.get(1).getName());
        Assert.assertFalse(people.get(1).isNative());
    }

    @Test
    public void testCsvWithTypePartial() throws IOException {
        Path path = getResourcePath("csv/person-partial.csv");
        List<Person> result = toList(new CsvReader().read(path, Person.class));
        assertPersonPojosPartial(result);
    }

    @Test
    public void testCsvWithCustomDateFormat() throws IOException {
        Path path = getResourcePath("csv/person-custom-format.csv");
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy.MM.dd");
        CsvReader reader = CsvReader.builder()
                .addParser(LocalDate.class.getTypeName(), LocalDate.class, s -> LocalDate.parse(s, formatter))
                .build();
        List<Person> result = toList(reader.read(path, Person.class));
        assertPersonPojos(result);
    }

    private void assertPersonDocuments(List<SpaceDocument> result, String expectedTypeName) {
        Assert.assertEquals(3, result.size());

        SpaceDocument entry1 = result.get(0);
        Assert.assertEquals(expectedTypeName, entry1.getTypeName());
        Assert.assertEquals(1, (int)entry1.getProperty("id"));
        Assert.assertEquals("john", entry1.getProperty("name"));
        Assert.assertEquals(LocalDate.parse("1970-01-01"), entry1.getProperty("birthday"));
        Assert.assertTrue(entry1.getProperty("native"));

        SpaceDocument entry2 = result.get(1);
        Assert.assertEquals(expectedTypeName, entry2.getTypeName());
        Assert.assertEquals(2, (int)entry2.getProperty("id"));
        Assert.assertEquals("jane", entry2.getProperty("name"));
        Assert.assertEquals(LocalDate.parse("1980-12-31"), entry2.getProperty("birthday"));
        Assert.assertFalse(entry2.getProperty("native"));

        SpaceDocument entry3 = result.get(2);
        Assert.assertEquals(expectedTypeName, entry3.getTypeName());
        Assert.assertEquals(3, (int)entry3.getProperty("id"));
        Assert.assertFalse(entry3.containsProperty("name"));
        Assert.assertFalse(entry3.containsProperty("birthday"));
        Assert.assertFalse(entry3.containsProperty("native"));
    }

    private void assertPersonPojos(List<Person> result) {
        Assert.assertEquals(3, result.size());

        Person entry1 = result.get(0);
        Assert.assertEquals(1, entry1.getId());
        Assert.assertEquals("john", entry1.getName());
        Assert.assertEquals(LocalDate.parse("1970-01-01"), entry1.getBirthday());
        Assert.assertTrue(entry1.isNative());

        Person entry2 = result.get(1);
        Assert.assertEquals(2, entry2.getId());
        Assert.assertEquals("jane", entry2.getName());
        Assert.assertEquals(LocalDate.parse("1980-12-31"), entry2.getBirthday());
        Assert.assertFalse(entry2.isNative());

        Person entry3 = result.get(2);
        Assert.assertEquals(3, entry3.getId());
        Assert.assertNull(entry3.getName());
        Assert.assertNull(entry3.getBirthday());
        Assert.assertFalse(entry3.isNative());
    }

    private void assertPersonPojosPartial(List<Person> result) {
        Assert.assertEquals(3, result.size());

        Person entry1 = result.get(0);
        Assert.assertEquals(1, entry1.getId());
        Assert.assertEquals("john", entry1.getName());
        Assert.assertNull(entry1.getBirthday());
        Assert.assertFalse(entry1.isNative());

        Person entry2 = result.get(1);
        Assert.assertEquals(2, entry2.getId());
        Assert.assertEquals("jane", entry2.getName());
        Assert.assertNull(entry2.getBirthday());
        Assert.assertFalse(entry2.isNative());

        Person entry3 = result.get(2);
        Assert.assertEquals(3, entry3.getId());
        Assert.assertNull(entry3.getName());
        Assert.assertNull(entry3.getBirthday());
        Assert.assertFalse(entry3.isNative());
    }

    private Path getResourcePath(String resource) {
        try {
            return Paths.get(getClass().getResource("/" + resource).toURI());
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    private static <T> List<T> toList(Stream<T> stream) {
        List<T> list = stream.collect(Collectors.toList());
        stream.close();
        return list;
    }

    private static class Person {
        private int id;
        private String name;
        private LocalDate birthday;
        private boolean _native;

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public LocalDate getBirthday() {
            return birthday;
        }

        public void setBirthday(LocalDate birthday) {
            this.birthday = birthday;
        }

        public boolean isNative() {
            return _native;
        }

        public void setNative(boolean _native) {
            this._native = _native;
        }
    }
}
