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


package com.gigaspaces.internal.io;

import com.gigaspaces.internal.utils.yaml.YamlParserFactory;
import com.gigaspaces.internal.utils.yaml.YamlUtils;

import java.io.*;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.function.BooleanSupplier;
import java.util.stream.Stream;

/**
 * This class provides a set of static utility methods used for I/O manipulations. (writing/reading
 * objects to and from streams)
 *
 * @author elip
 * @version 9.1.1
 */
@com.gigaspaces.api.InternalApi
public class BootIOUtils {

    private static final byte STRING_NULL = 0;
    private static final byte STRING_UTF = 1;
    private static final byte STRING_OBJECT = 2;
    /*
     * out.writeUTF throws an exception is the number of bytes required to write
     * the string is more than 65535. To be on the safe side, we set the limit
     * to half of that, in case all the characters require 2 bytes each.
     * (Theoretically, this is not good enough, since characters might take
     * more).
     */
    public static final int UTF_MAX_LENGTH = 32767;

    public static final String NEW_LINE = System.getProperty("line.separator");

    public static boolean isEmpty(String s) {
        return s == null || s.isEmpty();
    }

    public static String quoteIfContainsSpace(String s) {
        final String QUOTE = "\"";
        return s.contains(" ") && !s.startsWith(QUOTE) && !s.endsWith(QUOTE) ? QUOTE + s + QUOTE : s;
    }

    public static void writeMapStringString(ObjectOutput out,
                                            Map<String, String> map) throws IOException {
        if (map == null)
            out.writeInt(-1);
        else {
            int length = map.size();
            out.writeInt(length);
            for (Entry<String, String> entry : map.entrySet()) {
                writeString(out, entry.getKey());
                writeString(out, entry.getValue());
            }
        }
    }

    public static void writeProperties(ObjectOutput out, Properties properties) throws IOException {
        if (properties == null)
            out.writeInt(-1);
        else {
            int length = properties.size();
            out.writeInt(length);
            for (Entry<Object, Object> entry : properties.entrySet()) {
                writeString(out, (String) entry.getKey());
                writeString(out, (String) entry.getValue());
            }
        }
    }

    public static void writeString(ObjectOutput out, String s)
            throws IOException {
        if (s == null)
            out.writeByte(STRING_NULL);
        else if (s.length() < UTF_MAX_LENGTH) {
            out.writeByte(STRING_UTF);
            out.writeUTF(s);
        } else {
            out.writeByte(STRING_OBJECT);
            out.writeObject(s);
        }
    }

    public static Map<String, String> readMapStringString(ObjectInput in)
            throws IOException, ClassNotFoundException {
        Map<String, String> map = null;

        int length = in.readInt();
        if (length >= 0) {
            map = new HashMap<String, String>(length);
            for (int i = 0; i < length; i++) {
                String key = readString(in);
                String value = readString(in);
                map.put(key, value);
            }
        }

        return map;
    }

    public static Properties readProperties(ObjectInput in)
            throws IOException, ClassNotFoundException {
        Properties properties = null;

        int length = in.readInt();
        if (length >= 0) {
            properties = new Properties();
            for (int i = 0; i < length; i++) {
                String key = readString(in);
                String value = readString(in);
                properties.setProperty(key, value);
            }
        }

        return properties;
    }

    public static String readString(ObjectInput in) throws IOException,
            ClassNotFoundException {
        byte code = in.readByte();
        switch (code) {
            case STRING_NULL:
                return null;
            case STRING_UTF:
                String s = in.readUTF();
                return s;
            case STRING_OBJECT:
                Object obj = in.readObject();
                return (String) obj;
            default:
                throw new IllegalStateException(
                        "Failed to deserialize a string: unrecognized string type code - "
                                + code);
        }
    }

    public static String[] readStringArray(ObjectInput in) throws IOException,
            ClassNotFoundException {
        String[] array = null;

        int length = in.readInt();
        if (length >= 0) {
            array = new String[length];
            for (int i = 0; i < length; i++)
                array[i] = readString(in);
        }

        return array;
    }

    public static void writeStringArray(ObjectOutput out, String[] array)
            throws IOException {
        if (array == null)
            out.writeInt(-1);
        else {
            int length = array.length;
            out.writeInt(length);
            for (int i = 0; i < length; i++)
                writeString(out, array[i]);
        }
    }

    /**
     * A replacement for {@link File#listFiles()} that does not return null
     *
     * @throws IllegalArgumentException if not a directory or has no read permissions
     */
    public static File[] listFiles(File dir) {
        if (!dir.isDirectory())
            throw new IllegalArgumentException(dir.getPath() + " is not a directory");
        if (!dir.canRead())
            throw new IllegalArgumentException("No read permissions for " + dir.getPath());
        final File[] files = dir.listFiles();
        if (files == null) {
            //see http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=4505804
            throw new IllegalArgumentException("An unknown i/o error occurred when scanning files in directory " + dir.getPath());
        }
        return files;
    }

    public static File[] listFiles(File dir, FileFilter filter) {
        if (!dir.isDirectory())
            throw new IllegalArgumentException(dir.getPath() + " is not a directory");
        if (!dir.canRead())
            throw new IllegalArgumentException("No read permissions for " + dir.getPath());
        final File[] files = dir.listFiles(filter);
        if (files == null) {
            //see http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=4505804
            throw new IllegalArgumentException("An unknown i/o error occurred when scanning files in directory " + dir.getPath());
        }
        return files;
    }

    public static File[] listFiles(File dir, FilenameFilter filter) {
        if (!dir.isDirectory())
            throw new IllegalArgumentException(dir.getPath() + " is not a directory");
        if (!dir.canRead())
            throw new IllegalArgumentException("No read permissions for " + dir.getPath());
        final File[] files = dir.listFiles(filter);
        if (files == null) {
            //see http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=4505804
            throw new IllegalArgumentException("An unknown i/o error occurred when scanning files in directory " + dir.getPath());
        }
        return files;
    }

    public static String wrapIpv6HostAddressIfNeeded(InetAddress hostAddress) {
        if (!(hostAddress instanceof Inet6Address)) {
            return hostAddress.getHostAddress();
        }
        return "[" + hostAddress.getHostAddress() + "]";
    }

    public static String path(String base, String...tokens) {
        String result = base.endsWith(File.separator) ? base.substring(0, base.length()-1) : base;
        for (String token : tokens)
            result += File.separator + token;
        return result;
    }


    public static Long parseStringAsBytes(String property) {
        if (isEmpty(property))
            return null;

        // Find first non-digit char:
        int pos = 0;
        while (pos < property.length() && Character.isDigit(property.charAt(pos)))
            pos++;

        String prefix = property.substring(0, pos);
        long number = Long.parseLong(prefix);
        String suffix = pos < property.length() ? property.substring(pos) : null;
        int factor = parseMemoryUnit(suffix);
        return number * factor;
    }

    private static int parseMemoryUnit(String s) {
        if (s == null) return 1;
        if (s.equalsIgnoreCase("b")) return 1;
        if (s.equalsIgnoreCase("k")) return 1024;
        if (s.equalsIgnoreCase("ki")) return 1024;
        if (s.equalsIgnoreCase("kib")) return 1024;
        if (s.equalsIgnoreCase("kb")) return 1000;
        if (s.equalsIgnoreCase("m")) return 1024*1024;
        if (s.equalsIgnoreCase("mi")) return 1024*1024;
        if (s.equalsIgnoreCase("mib")) return 1024*1024;
        if (s.equalsIgnoreCase("mb")) return 1000*1000;
        if (s.equalsIgnoreCase("g")) return 1024*1024*1024;
        if (s.equalsIgnoreCase("gi")) return 1024*1024*1024;
        if (s.equalsIgnoreCase("gib")) return 1024*1024*1024;
        if (s.equalsIgnoreCase("gb")) return 1000*1000*1000;
        if (s.equalsIgnoreCase("t")) return 1024*1024*1024*1024;
        if (s.equalsIgnoreCase("ti")) return 1024*1024*1024*1024;
        if (s.equalsIgnoreCase("tib")) return 1024*1024*1024*1024;
        if (s.equalsIgnoreCase("tb")) return 1000*1000*1000*1000;
        throw new IllegalArgumentException("Invalid memory unit: '" + s + "'. Supported units: b, k, ki, kib, kb, m, mi, mib, mb, g, gi, gib, gb, t, ti, tib, tb");
    }

    public static boolean waitFor(BooleanSupplier predicate, long timeout, long pollInterval) throws InterruptedException {
        final long deadline = System.currentTimeMillis() + timeout;
        while (true) {
            if (predicate.getAsBoolean())
                return true;
            long currTime = System.currentTimeMillis();
            if (currTime >= deadline)
                return false;
            Thread.sleep(Math.min(pollInterval, deadline - currTime));
        }
    }

    public static boolean tryWaitFor(BooleanSupplier predicate, long timeout, long pollInterval) {
        try {
            return waitFor(predicate, timeout, pollInterval);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    public static File locate(String baseDir, String fileName) {
        if (baseDir.startsWith("$")) {
            String sysProp = baseDir.substring(1);
            baseDir = System.getProperty(sysProp);
            if (baseDir == null)
                throw new IllegalArgumentException("baseDir [" + sysProp + "] does not reference a valid System Property");
        }
        return locate(new File(baseDir), fileName);
    }

    public static File locate(File baseDir, String fileName) {
        if (baseDir == null)
            throw new IllegalArgumentException("baseDir is null");
        if (fileName == null)
            throw new IllegalArgumentException("subDirName is null");

        File file = new File(baseDir, fileName);
        if (file.exists())
            return file;

        for (File f : BootIOUtils.listFiles(baseDir)) {
            if (f.isDirectory() && f.canRead()) {
                file = locate(f, fileName);
                if (file != null)
                    return file;
            }
        }

        return null;
    }


    public static Stream<String> lines(InputStream inputStream) {
        return lines(inputStream, StandardCharsets.UTF_8);
    }

    public static Stream<String> lines(InputStream inputStream, Charset charset) {
        BufferedReader br = new BufferedReader(new InputStreamReader(inputStream, charset));

        try {
            return br.lines().onClose(asUncheckedRunnable(br));
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

    public static Runnable asUncheckedRunnable(Closeable c) {
        return () -> {
            try {
                c.close();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        };
    }

    public static boolean isYaml(String path) {
        return path.endsWith(".yaml") || path.endsWith(".yml");
    }

    public static Properties loadProperties(Path path) throws IOException {
        try (InputStream stream = Files.newInputStream(path)) {
            return loadProperties(stream, path.toString());
        }
    }

    public static Properties loadProperties(InputStream stream, String name) throws IOException {
        return isYaml(name)
                ? YamlUtils.toProperties(YamlParserFactory.create().parse(stream))
                : loadProperties(stream);
    }

    public static Properties loadProperties(InputStream stream) throws IOException {
        Properties result = new Properties();
        result.load(stream);
        return result;
    }

    public static Path getResourcePath(String resource) throws IOException {
        return getResourcePath(resource, Thread.currentThread().getContextClassLoader());
    }

    public static Path getResourcePath(String resource, ClassLoader classLoader) throws IOException {
        URL url = classLoader != null ? classLoader.getResource(resource) : ClassLoader.getSystemResource(resource);
        try {
            return Paths.get(url.toURI());
        } catch (URISyntaxException e) {
            throw new IOException(e);
        }
    }

    public static InputStream getResourceAsStream(String resource) {
        return getResourceAsStream(resource, Thread.currentThread().getContextClassLoader());
    }

    public static InputStream getResourceAsStream(String resource, ClassLoader classLoader) {
        return classLoader != null ? classLoader.getResourceAsStream(resource) : ClassLoader.getSystemResourceAsStream(resource);
    }

    public static String readAsString(Path path) throws IOException {
        return new String(Files.readAllBytes(path));
    }

    public static String readAsString(InputStream is) throws IOException {
        BufferedReader input = new BufferedReader( new InputStreamReader( is ) );
        String line;
        StringBuilder content = new StringBuilder();
        while ((line = input.readLine()) != null) {
            content.append( line );
            content.append( '\n' );
        }
        input.close();
        return content.toString();
    }


    public static void deleteRecursive(Path path) throws IOException {
        try {
            Files.walk(path).sorted(Comparator.reverseOrder())
                    .forEach(p -> {
                        try {
                            Files.delete(p);
                        } catch (IOException e) {
                            throw new UncheckedIOException("Failed to delete " + p, e);
                        }
                    });
        }catch (UncheckedIOException e){
            throw e.getCause();
        }
    }

    public static boolean isURL(String path) {
        final String pathTrimmed = path.trim().toLowerCase();
        return pathTrimmed.startsWith("http://") || pathTrimmed.startsWith("https://");
    }
}
