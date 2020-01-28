package org.gigaspaces.blueprints;

import org.junit.Assert;
import org.junit.Test;

import java.io.Closeable;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.*;

public class BlueprintsTestCase {
    @Test
    public void testInvalidHome() throws IOException {
        String invalidPath = "no-such-path";
        try {
            new BlueprintRepository(Paths.get(invalidPath));
            Assert.fail("Should have failed - invalid path");
        } catch (IllegalArgumentException e) {
            Assert.assertThat(e.getMessage(), containsString(invalidPath));
        }
    }

    @Test
    public void testNames() throws IOException {
        Path templates = getResourcePath("blueprints");
        BlueprintRepository repository = new BlueprintRepository(templates);
        Assert.assertEquals(Files.list(templates).count(), repository.getNames().size());
        Assert.assertTrue(repository.getNames().contains("sample"));
    }

    @Test
    public void testCreate() throws IOException {
        BlueprintRepository repository = new BlueprintRepository(getResourcePath("blueprints"));
        Assert.assertFalse(Files.exists(Paths.get("output")));
        try (TempDirContext output = new TempDirContext("output")) {
            repository.get("sample").generate(output.path.resolve("my-sample"));
            assertContent(Paths.get("output", "my-sample", "file.txt"),
                    "Line 1: no macros",
                    "Line 2: single macro - Hello",
                    "Line 3: multiple macros - Hello and World"
                    );
            assertContent(Paths.get("output", "my-sample", "Hello", "World.txt"),
                    "Hello and World");
            assertContent(Paths.get("output", "my-sample", "com", "acme", "some-file.txt"),
                    "com.acme and some-file");
        }
        Assert.assertFalse(Files.exists(Paths.get("output")));
    }

    @Test
    public void testCreateInvalidTemplate() throws IOException {
        BlueprintRepository repository = new BlueprintRepository(getResourcePath("blueprints"));
        Assert.assertNull(repository.get("no-such-template"));
    }

    @Test
    public void testCreateAlreadyExists() throws IOException {
        BlueprintRepository repository = new BlueprintRepository(getResourcePath("blueprints"));
        Assert.assertFalse(Files.exists(Paths.get("output")));
        try (TempDirContext output = new TempDirContext("output")) {
            Path path = output.path.resolve("foo");
            Files.createDirectory(path);
            repository.get("sample").generate(path);
            Assert.fail("Should have failed - invalid template");
        } catch (IllegalArgumentException e) {
            Assert.assertThat(e.getMessage(), containsString(Paths.get("output", "foo").toString()));
        }
        Assert.assertFalse(Files.exists(Paths.get("output")));
    }

    private static void assertContent(Path path, String ... expectedLines) throws IOException {
        if (!Files.exists(path))
            Assert.fail("Path does not exist: " + path);
        try (Stream<String> stream = Files.lines(path)) {
            String[] actualLines = stream.toArray(String[]::new);
            Assert.assertArrayEquals(expectedLines, actualLines);
        }
    }

    private static class TempDirContext implements Closeable {

        private final Path path;

        private TempDirContext(String path) throws IOException {
            this(Paths.get(path));
        }

        private TempDirContext(Path path) throws IOException {
            this.path = path;
            Files.createDirectory(path);
        }

        @Override
        public void close() throws IOException {
            Files.walk(path).sorted(Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
            if (Files.exists(path))
                throw new IOException("Failed to delete " + path);
        }
    }

    private Path getResourcePath(String resource) {
        try {
            return Paths.get(getClass().getResource("/" + resource).toURI());
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }
}
