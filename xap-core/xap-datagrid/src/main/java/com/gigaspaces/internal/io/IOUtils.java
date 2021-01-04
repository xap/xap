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

import com.gigaspaces.executor.SpaceTask;
import com.gigaspaces.internal.collections.CollectionsFactory;
import com.gigaspaces.internal.collections.IntegerObjectMap;
import com.gigaspaces.internal.collections.ObjectIntegerMap;
import com.gigaspaces.internal.serialization.BooleanClassSerializer;
import com.gigaspaces.internal.serialization.ByteArrayClassSerializer;
import com.gigaspaces.internal.serialization.ByteClassSerializer;
import com.gigaspaces.internal.serialization.CharacterClassSerializer;
import com.gigaspaces.internal.serialization.DoubleClassSerializer;
import com.gigaspaces.internal.serialization.FloatClassSerializer;
import com.gigaspaces.internal.serialization.IClassSerializer;
import com.gigaspaces.internal.serialization.IntegerClassSerializer;
import com.gigaspaces.internal.serialization.LongClassSerializer;
import com.gigaspaces.internal.serialization.NullClassSerializer;
import com.gigaspaces.internal.serialization.ObjectClassSerializer;
import com.gigaspaces.internal.serialization.ShortClassSerializer;
import com.gigaspaces.internal.serialization.StringClassSerializer;
import com.gigaspaces.internal.server.space.redolog.storage.bytebuffer.ISwapExternalizable;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.internal.transport.ITemplatePacket;
import com.gigaspaces.lrmi.LRMIInvocationContext;
import com.gigaspaces.utils.CodeChangeUtilities;
import com.j_spaces.core.SpaceContext;
import com.j_spaces.kernel.ClassLoaderHelper;
import net.jini.core.transaction.Transaction;
import org.jini.rio.boot.CodeChangeClassLoadersManager;
import org.jini.rio.boot.ServiceClassLoader;
import org.jini.rio.boot.SupportCodeChangeAnnotationContainer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.StreamCorruptedException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.UnknownHostException;
import java.util.*;
import java.util.Map.Entry;

/**
 * This class provides a set of static utility methods used for I/O manipulations (convert data to
 * streams, socket ports).
 *
 * @author Igor Goldenberg
 * @author Guy Korland
 * @version 4.5
 */
@com.gigaspaces.api.InternalApi
public class IOUtils {
    private static ByteArrayOutputStream outStream = new ByteArrayOutputStream(65535);

    private static final Map<Class<?>, IClassSerializer<?>> _typeCache;
    private static final Map<Byte, IClassSerializer<?>> _codeCache;

    private static final ObjectIntegerMap<Class<?>> _classToCode = CollectionsFactory.getInstance().createObjectIntegerMap();
    private static final IntegerObjectMap<Class<?>> _codeToClass = CollectionsFactory.getInstance().createIntegerObjectMap();
    private static int _swapExtenKey = 0;

    private static final IClassSerializer<?> _defaultSerializer = new ObjectClassSerializer();

    static {
        _typeCache = new HashMap<Class<?>, IClassSerializer<?>>();
        _codeCache = new HashMap<Byte, IClassSerializer<?>>();

        // Register default serializer (by code only):
        _codeCache.put(_defaultSerializer.getCode(), _defaultSerializer);
        // Register special handler for null:
        register(null, new NullClassSerializer<Object>());
        // Register primitive types:
        register(Byte.class, new ByteClassSerializer());
        register(Short.class, new ShortClassSerializer());
        register(Integer.class, new IntegerClassSerializer());
        register(Long.class, new LongClassSerializer());
        register(Float.class, new FloatClassSerializer());
        register(Double.class, new DoubleClassSerializer());
        register(Boolean.class, new BooleanClassSerializer());
        register(Character.class, new CharacterClassSerializer());
        // Register common java types:
        register(String.class, new StringClassSerializer());
        register(byte[].class, new ByteArrayClassSerializer());

        //register(HashMap.class, new HashMapSerializer());

        //register((byte)10, TypeDesc.class, PlatformLogicalVersion.v7_1_0_ga);
//		register((byte)11, InactiveTypeDesc.class, PlatformLogicalVersion.v7_1_0_ga);
//		register((byte)12, ServerTypeDesc.class, PlatformLogicalVersion.v7_1_0_ga);
//		register((byte)13, PojoIntrospector.class, PlatformLogicalVersion.v7_1_0_ga);
//		register((byte)14, ExternalEntryIntrospector.class, PlatformLogicalVersion.v7_1_0_ga);
//		register((byte)15, EntryIntrospector.class, PlatformLogicalVersion.v7_1_0_ga);
//		register((byte)16, MetadataEntryIntrospector.class, PlatformLogicalVersion.v7_1_0_ga);
//		register((byte)17, PropertyInfo.class, PlatformLogicalVersion.v7_1_0_ga);
//		register((byte)18, IdentifierInfo.class, PlatformLogicalVersion.v7_1_0_ga);
//		register((byte)19, SpacePropertyIndex.class, PlatformLogicalVersion.v7_1_0_ga);
//		register((byte)20, CustomIndex.class, PlatformLogicalVersion.v7_1_0_ga);
    }

    private static void register(Class<?> type, IClassSerializer<?> serializer) {
        _typeCache.put(type, serializer);
        _codeCache.put(serializer.getCode(), serializer);
    }

    public static boolean isArchive(String fileName) {
        return fileName.endsWith(".zip") || fileName.endsWith(".jar") || fileName.endsWith(".war");
    }

    /**
     * Creates an object from a byte buffer.
     **/
    public static Object objectFromByteBuffer(byte[] buffer)
            throws Exception {
        if (buffer == null)
            return null;

        ByteArrayInputStream inStream = new ByteArrayInputStream(buffer);
        ObjectInputStream in = new ObjectInputStream(inStream);
        Object retval = in.readObject();
        in.close();

        return retval;
    }

    /**
     * Serializes an object into a byte buffer. The object has to implement interface Serializable
     * or Externalizable.
     **/
    public static byte[] objectToByteBuffer(Object obj)
            throws Exception {
        byte[] result = null;
        synchronized (outStream) {
            outStream.reset();
            ObjectOutputStream out = new ObjectOutputStream(outStream);
            out.writeObject(obj);
            out.flush();
            result = outStream.toByteArray();
            out.close();
        }

        return result;
    }

    /**
     * A deep copy makes a distinct copy of each of the object's fields, recursing through the
     * entire graph of other objects referenced by the object being copied. Deep clone by serialize
     * and deserialize the object and return the deserialized version. A deep copy/clone, assuming
     * everything in the tree is serializable.
     *
     * NOTE: This method is very expensive!, don't use this method if you need performance.
     *
     * @param obj the object to clone, the object and object context must implement
     *            java.io.Serializable.
     * @return the copied object include all object references.
     * @throws IllegalArgumentException Failed to perform deep clone. The object of the context
     *                                  object is not implements java.io.Serializable.
     **/
    public static Object deepClone(Object obj) {
        try {
            byte[] bArray = objectToByteBuffer(obj);
            return objectFromByteBuffer(bArray);
        } catch (Exception ex) {
            throw new IllegalArgumentException("Failed to perform deep clone on [" + obj + "] object. Check that the all object context are implements java.io.Serializable.", ex);
        }
    }

    public static void writeUUID(ObjectOutput out, UUID value) throws IOException {
        out.writeLong(value.getLeastSignificantBits());
        out.writeLong(value.getMostSignificantBits());
    }

    public static UUID readUUID(ObjectInput in) throws IOException {
        long least = in.readLong();
        long most = in.readLong();
        return new UUID(most, least);
    }

    final public static class NoHeaderObjectOutputStream
            extends ObjectOutputStream {
        public NoHeaderObjectOutputStream(OutputStream out) throws IOException {
            super(out);
        }

        @Override
        protected void writeStreamHeader() throws IOException {
            writeByte(TC_RESET);
        }
    }

    final public static class NoHeaderObjectInputStream
            extends ObjectInputStream {
        public NoHeaderObjectInputStream(InputStream in) throws IOException {
            super(in);
        }

        @Override
        protected void readStreamHeader() throws IOException, StreamCorruptedException {
        }
    }

    /**
     * Marshals <code>value</code> to an <code>ObjectOutputStream</code> stream, <code>out</code>,
     * using RMI's serialization format for arguments or return values.  For primitive types, the
     * primitive type's class should be specified (i.e., for the type <code>int</code>, specify
     * <code>int.class</code>), and the primitive value should be wrapped in instances of the
     * appropriate wrapper class, such as <code>java.lang.Integer</code> or
     * <code>java.lang.Boolean</code>.
     *
     * @param type  <code>Class</code> object for the value to be marshalled
     * @param value value to marshal
     * @param out   stream to which the value is marshalled
     * @throws IOException if an I/O error occurs marshalling the value to the output stream
     **/
    public static void marshalValue(Class type, Object value, ObjectOutput out)
            throws IOException {
        //TODO cache isPrimitive because it appear in profiling
        if (type.isPrimitive()) {
            if (type == int.class) {
                out.writeInt(((Integer) value).intValue());
            } else if (type == boolean.class) {
                out.writeBoolean(((Boolean) value).booleanValue());
            } else if (type == long.class) {
                out.writeLong(((Long) value).longValue());
            } else if (type == short.class) {
                out.writeShort(((Short) value).shortValue());
            } else if (type == float.class) {
                out.writeFloat(((Float) value).floatValue());
            } else if (type == double.class) {
                out.writeDouble(((Double) value).doubleValue());
            } else if (type == char.class) {
                out.writeChar(((Character) value).charValue());
            } else if (type == byte.class) {
                out.writeByte(((Byte) value).byteValue());
            } else {
                throw new AssertionError("Unrecognized primitive type: " + type);
            }
        } else {
            out.writeObject(value);
        }
    }

    /**
     * Unmarshals a value of the specified <code>type</code> from the <code>ObjectInputStream</code>
     * stream, <code>in</code>, using RMI's serialization format for arguments or return values and
     * returns the result.  For primitive types, the primitive type's class should be specified
     * (i.e., for the primitive type <code>int</code>, specify <code>int.class</code>).
     *
     * @param type <code>Class</code> object for the value to be unmarshalled
     * @param in   stream from which the value is unmarshalled
     * @return value unmarshalled from the input stream
     * @throws IOException            if an I/O error occurs marshalling the value to the output
     *                                stream
     * @throws ClassNotFoundException if the <code>type</code>'s class could not	be found
     **/
    public static Object unmarshalValue(Class type, ObjectInput in)
            throws IOException, ClassNotFoundException {
        if (type.isPrimitive()) {
            if (type == int.class)
                return in.readInt();
            if (type == boolean.class)
                return in.readBoolean();
            if (type == long.class)
                return in.readLong();
            if (type == short.class)
                return in.readShort();
            if (type == float.class)
                return in.readFloat();
            if (type == double.class)
                return in.readDouble();
            if (type == char.class)
                return in.readChar();
            if (type == byte.class)
                return in.readByte();

            throw new AssertionError("Unrecognized primitive type: " + type);
        }
        return in.readObject();
    }


    /**
     * Checks whether a supplied socket port is busy. The port must be between 0 and 65535,
     * inclusive.
     *
     * @param port     the port to check.
     * @param bindAddr check if port busy on specific {@link InetAddress}, If <i>bindAddr</i> is
     *                 null, it will default accepting connections on any/all local addresses
     * @return <code>true</code> if supplied port is busy, otherwise <code>false</code>.
     * @throws UnknownHostException if no IP address for the <code>host</code> could be found, or if
     *                              a scope_id was specified for a global IPv6 address.
     **/
    public static boolean isPortBusy(int port, String bindAddr) throws UnknownHostException {
        InetAddress inetBindAddr = bindAddr != null ? InetAddress.getByName(bindAddr) : null;

        try {
            new ServerSocket(port, 0, inetBindAddr).close();
        } catch (IOException ex) {
            return true;
        }

        return false;
    }


    /**
     * Get an anonymous socket port.
     *
     * @return An anonymous port created by instantiating a <code>java.net.ServerSocket</code> with
     * a port of 0
     */
    public static int getAnonymousPort() throws java.io.IOException {
        java.net.ServerSocket socket = new java.net.ServerSocket(0);
        int port = socket.getLocalPort();
        socket.close();

        return port;
    }

    public static void writeShortArray(ObjectOutput out, short[] array)
            throws IOException {
        if (array == null)
            out.writeInt(-1);
        else {
            int length = array.length;
            out.writeInt(length);
            for (int i = 0; i < length; i++)
                out.writeShort(array[i]);
        }
    }

    public static short[] readShortArray(ObjectInput in)
            throws IOException {
        short[] array = null;

        int length = in.readInt();
        if (length >= 0) {
            array = new short[length];
            for (int i = 0; i < length; i++)
                array[i] = in.readShort();
        }

        return array;
    }

    public static void writeIntegerArray(ObjectOutput out, int[] array)
            throws IOException {
        if (array == null)
            out.writeInt(-1);
        else {
            int length = array.length;
            out.writeInt(length);
            for (int i = 0; i < length; i++)
                out.writeInt(array[i]);
        }
    }

    public static int[] readIntegerArray(ObjectInput in)
            throws IOException {
        int[] array = null;

        int length = in.readInt();
        if (length >= 0) {
            array = new int[length];
            for (int i = 0; i < length; i++)
                array[i] = in.readInt();
        }

        return array;
    }

    public static void writeLongArray(ObjectOutput out, long[] array)
            throws IOException {
        if (array == null)
            out.writeInt(-1);
        else {
            int length = array.length;
            out.writeInt(length);
            for (int i = 0; i < length; i++)
                out.writeLong(array[i]);
        }
    }

    public static long[] readLongArray(ObjectInput in) throws IOException {
        long[] array = null;

        int length = in.readInt();
        if (length >= 0) {
            array = new long[length];
            for (int i = 0; i < length; i++)
                array[i] = in.readLong();
        }

        return array;
    }

    public static void writeByteArray(ObjectOutput out, byte[] array) throws IOException {
        if (array == null)
            out.writeInt(-1);
        else {
            out.writeInt(array.length);
            out.write(array);
        }
    }

    public static byte[] readByteArray(ObjectInput in)
            throws IOException {
        int length = in.readInt();
        if (length == -1)
            return null;
        byte[] array = new byte[length];
        in.readFully(array);

        return array;
    }

    public static void writeBooleanArray(ObjectOutput out, boolean[] array)
            throws IOException {
        if (array == null)
            out.writeInt(-1);
        else {
            int length = array.length;
            out.writeInt(length);
            for (int i = 0; i < length; i++)
                out.writeBoolean(array[i]);
        }
    }

    public static boolean[] readBooleanArray(ObjectInput in)
            throws IOException {
        boolean[] array = null;

        int length = in.readInt();
        if (length >= 0) {
            array = new boolean[length];
            for (int i = 0; i < length; i++)
                array[i] = in.readBoolean();
        }

        return array;
    }

    /**
     * Shrink string over the wire, should be used for constant number of strings which are
     * repetitive (i.e space names, class names)
     */
    public static void writeRepetitiveString(ObjectOutput out, String s) throws IOException {
        if (out instanceof MarshalOutputStream)
            ((MarshalOutputStream) out).writeRepetitiveObject(s);
        else
            writeString(out, s);
    }

    /**
     * Read strings that were Shrinked using {@link #writeRepetitiveString(ObjectOutput, String)}
     */
    public static String readRepetitiveString(ObjectInput in) throws IOException, ClassNotFoundException {
        if (in instanceof MarshalInputStream)
            return (String) ((MarshalInputStream) in).readRepetitiveObject();

        return readString(in);
    }

    public static void writeString(ObjectOutput out, String s)
            throws IOException {
        BootIOUtils.writeString(out, s);
    }

    public static String readString(ObjectInput in)
            throws IOException, ClassNotFoundException {
        return BootIOUtils.readString(in);
    }

    public static void writeStringArray(ObjectOutput out, String[] array)
            throws IOException {
        BootIOUtils.writeStringArray(out, array);
    }

    public static String[] readStringArray(ObjectInput in)
            throws IOException, ClassNotFoundException {
        String[] array = null;

        int length = in.readInt();
        if (length >= 0) {
            array = new String[length];
            for (int i = 0; i < length; i++)
                array[i] = readString(in);
        }

        return array;
    }

    public static void writeStringSet(ObjectOutput out,
                                      Set<String> set) throws IOException {
        if (set == null)
            out.writeInt(-1);
        else {
            int length = set.size();
            out.writeInt(length);
            for (String str : set)
                writeString(out, str);
        }
    }

    public static Set<String> readStringSet(ObjectInput in)
            throws IOException, ClassNotFoundException {
        Set<String> set = null;

        int length = in.readInt();
        if (length >= 0) {
            set = new HashSet<String>();
            for (int i = 0; i < length; i++)
                set.add(readString(in));
        }

        return set;
    }

    public static void writeRepetitiveStringArray(ObjectOutput out, String[] array)
            throws IOException {
        if (array == null)
            out.writeInt(-1);
        else {
            int length = array.length;
            out.writeInt(length);
            for (int i = 0; i < length; i++)
                writeRepetitiveString(out, array[i]);
        }
    }

    public static String[] readRepetitiveStringArray(ObjectInput in)
            throws IOException, ClassNotFoundException {
        String[] array = null;

        int length = in.readInt();
        if (length >= 0) {
            array = new String[length];
            for (int i = 0; i < length; i++)
                array[i] = readRepetitiveString(in);
        }

        return array;
    }

    public static void writeList(ObjectOutput out, List list)
            throws IOException {
        if (list == null)
            out.writeInt(-1);
        else {
            int length = list.size();
            out.writeInt(length);
            for (int i = 0; i < length; i++)
                writeObject(out, list.get(i));
        }
    }

    public static List readList(ObjectInput in)
            throws IOException, ClassNotFoundException {
        List list = null;

        int length = in.readInt();
        if (length >= 0) {
            list = new ArrayList(length);
            for (int i = 0; i < length; i++)
                list.add(readObject(in));
        }

        return list;
    }

    public static void writeListString(ObjectOutput out, List<String> list)
            throws IOException {
        if (list == null)
            out.writeInt(-1);
        else {
            int length = list.size();
            out.writeInt(length);
            for (int i = 0; i < length; i++)
                writeString(out, list.get(i));
        }
    }

    public static List<String> readListString(ObjectInput in)
            throws IOException, ClassNotFoundException {
        List<String> list = null;

        int length = in.readInt();
        if (length >= 0) {
            list = new ArrayList<String>(length);
            for (int i = 0; i < length; i++)
                list.add(readString(in));
        }

        return list;
    }

    public static void writeMapStringString(ObjectOutput out, Map<String, String> map)
            throws IOException {
        BootIOUtils.writeMapStringString(out, map);
    }

    public static Map<String, String> readMapStringString(ObjectInput in)
            throws IOException, ClassNotFoundException {
        return BootIOUtils.readMapStringString(in);
    }

    public static void writeMapStringObject(ObjectOutput out,
                                            Map<String, Object> map) throws IOException {
        if (map == null)
            out.writeInt(-1);
        else {
            int length = map.size();
            out.writeInt(length);
            for (Entry<String, Object> entry : map.entrySet()) {
                writeString(out, entry.getKey());
                writeObject(out, entry.getValue());
            }
        }
    }

    public static Map<String, Object> readMapStringObject(ObjectInput in)
            throws IOException, ClassNotFoundException {
        Map<String, Object> map = null;

        int length = in.readInt();
        if (length >= 0) {
            map = new HashMap<String, Object>(length);
            for (int i = 0; i < length; i++) {
                String key = readString(in);
                Object value = readObject(in);
                map.put(key, value);
            }
        }

        return map;
    }

    public static <T> void writeMapRepetitiveKeys(ObjectOutput out,
                                              Map<String, T> map) throws IOException {
        if (map == null)
            out.writeInt(-1);
        else {
            int length = map.size();
            out.writeInt(length);
            for (Entry<String, T> entry : map.entrySet()) {
                writeRepetitiveString(out, entry.getKey());
                writeObject(out, entry.getValue());
            }
        }
    }

    public static <T> Map<String, T> readMapRepetitiveKeys(ObjectInput in)
            throws IOException, ClassNotFoundException {
        Map<String, T> map = null;

        int length = in.readInt();
        if (length >= 0) {
            map = new HashMap<>(length);
            for (int i = 0; i < length; i++) {
                String key = readRepetitiveString(in);
                T value = readObject(in);
                map.put(key, value);
            }
        }

        return map;
    }


    /**
     * Should only be used for objects that their class is known to SystemJars.DATA_GRID_JAR,
     * meaning at SystemJars.DATA_GRID_JAR, its dependencies or JDK Objects read and written with
     * repetitive must be immutable (cannot be changed as they are kept in underlying map, changing
     * them will affect the next repetitiveRead/Write
     */
    public static void writeRepetitiveObject(ObjectOutput out, Object obj) throws IOException {
        if (out instanceof MarshalOutputStream)
            ((MarshalOutputStream) out).writeRepetitiveObject(obj);
        else
            writeObject(out, obj);
    }

    public static void writeObject(ObjectOutput out, Object obj)
            throws IOException {
        // Get object type:
        Class<?> type = obj != null ? obj.getClass() : null;
        // Get serializer by type:
        IClassSerializer serializer = _typeCache.get(type);
        // If type does not have serializer, or serializer is not supported in target version, use default serializer:
        if (serializer == null)
            serializer = _defaultSerializer;
        // Write type code:
        out.writeByte(serializer.getCode());
        // Serialize object using serializer:
        serializer.write(out, obj);
    }

    /**
     * Objects read and written with repetitive must be immutable (cannot be changed as they are
     * kept in underlying map, changing them will affect the next repetitiveRead/Write
     */
    public static <T> T readRepetitiveObject(ObjectInput in) throws IOException, ClassNotFoundException {
        if (in instanceof MarshalInputStream)
            return (T) ((MarshalInputStream) in).readRepetitiveObject();

        return (T) readObject(in);
    }

    public static <T> T readObject(ObjectInput in)
            throws IOException, ClassNotFoundException {
        // Read type code:
        final byte code = in.readByte();
        // Get serializer by code:
        IClassSerializer<?> serializer = _codeCache.get(code);
        if (serializer == null)
            throw new ClassNotFoundException("Unknown class code: " + code);
        // Read using serializer:
        Object result = serializer.read(in);
        // Cast and return result:
        return (T) result;
    }

    public static void writeObjectArray(ObjectOutput out, Object[] array)
            throws IOException {
        if (array == null)
            out.writeInt(-1);
        else {
            int length = array.length;
            out.writeInt(length);
            int i = 0;
            try {
                for (; i < length; i++)
                    writeObject(out, array[i]);
            } catch (IOException e) {
                throw new IOArrayException(i, "Failed to serialize item #" + i, e);
            }
        }
    }

    public static Object[] readObjectArray(ObjectInput in)
            throws IOException, ClassNotFoundException {
        final int length = in.readInt();
        if (length < 0)
            return null;

        Object[] array = new Object[length];
        int i = 0;
        try {
            for (; i < length; i++)
                array[i] = readObject(in);
        } catch (IOException e) {
            throw new IOArrayException(i, "Failed to deserialize item #" + i, e);
        }

        return array;
    }

    public static Throwable[] readThrowableArray(ObjectInput in)
            throws IOException, ClassNotFoundException {
        final int length = in.readInt();
        if (length < 0)
            return null;

        final Throwable[] array = new Throwable[length];
        for (int i = 0; i < length; i++)
            array[i] = readObject(in);
        return array;
    }

    public static Exception[] readExceptionArray(ObjectInput in)
            throws IOException, ClassNotFoundException {
        final int length = in.readInt();
        if (length < 0)
            return null;

        final Exception[] array = new Exception[length];
        for (int i = 0; i < length; i++)
            array[i] = readObject(in);
        return array;
    }

    public static IEntryPacket[] readEntryPacketArray(ObjectInput in)
            throws IOException, ClassNotFoundException {
        final int length = in.readInt();
        if (length < 0)
            return null;

        final IEntryPacket[] array = new IEntryPacket[length];
        for (int i = 0; i < length; i++)
            array[i] = readObject(in);
        return array;
    }

    public static ITemplatePacket[] readTemplatePacketArray(ObjectInput in)
            throws IOException, ClassNotFoundException {
        final int length = in.readInt();
        if (length < 0)
            return null;

        final ITemplatePacket[] array = new ITemplatePacket[length];
        for (int i = 0; i < length; i++)
            array[i] = readObject(in);
        return array;
    }

    public static void writeObjectArrayCompressed(ObjectOutput out, Object[] array)
            throws IOException {
        final int length = array.length;

        int numNonNullFields = 0;
        for (int i = 0; i < length; i++)
            if (array[i] != null)
                numNonNullFields++;

        final boolean isCompressed = numNonNullFields < length / 2;
        out.writeBoolean(isCompressed);

        if (isCompressed) {
            out.writeInt(length);
            out.writeInt(numNonNullFields);
            int i = 0;
            try {
                for (; i < length; i++) {
                    if (array[i] != null) {
                        out.writeInt(i);
                        writeObject(out, array[i]);
                    }
                }
            } catch (IOException e) {
                throw new IOArrayException(i, "Failed to serialize item #" + i, e);
            }
        } else
            writeObjectArray(out, array);
    }

    public static Object[] readObjectArrayCompressed(ObjectInput in)
            throws IOException, ClassNotFoundException {
        Object[] array;

        final boolean isCompressed = in.readBoolean();

        if (isCompressed) {
            final int length = in.readInt();
            array = new Object[length];
            final int numNonNullFields = in.readInt();
            int i = 0;
            try {
                for (; i < numNonNullFields; i++) {
                    int pos = in.readInt();
                    array[pos] = readObject(in);
                }
            } catch (IOException e) {
                throw new IOArrayException(i, "Failed to deserialize item #" + i, e);
            }
        } else
            array = readObjectArray(in);

        return array;
    }

    public static void writeSwapExternalizableObject(ObjectOutput out, ISwapExternalizable swapExternalizable) throws IOException {
        Class<? extends ISwapExternalizable> clazz = swapExternalizable.getClass();
        int code = getCode(clazz);
        out.writeInt(code);
        swapExternalizable.writeToSwap(out);
        out.flush();
    }

    public static <T extends ISwapExternalizable> T readSwapExternalizableObject(ObjectInput in) throws IOException,
            ClassNotFoundException {
        int classCode = in.readInt();
        Class<?> clazz = getClass(classCode);
        try {
            T newInstance = (T) clazz.newInstance();
            newInstance.readFromSwap(in);
            return newInstance;
        } catch (IOException e) {
            throw e;
        } catch (ClassNotFoundException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException(e.getMessage(), e);
        }
    }

    public static void writeNullableSwapExternalizableObject(ObjectOutput out, ISwapExternalizable swapExternalizable) throws IOException {
        out.writeBoolean(swapExternalizable != null);
        if (swapExternalizable != null)
            writeSwapExternalizableObject(out, swapExternalizable);
    }

    public static <T extends ISwapExternalizable> T readNullableSwapExternalizableObject(ObjectInput in) throws IOException,
            ClassNotFoundException {
        if (!in.readBoolean())
            return null;
        return readSwapExternalizableObject(in);
    }

    public static void writeWithCachedStubs(ObjectOutput out, Object obj) throws IOException {
        LRMIInvocationContext currentContext = LRMIInvocationContext.getCurrentContext();
        boolean previousStubCacheState = currentContext.isUseStubCache();
        currentContext.setUseStubCache(true);
        try {
            writeObject(out, obj);
        } finally {
            currentContext.setUseStubCache(previousStubCacheState);
        }
    }

    public static <T> T readWithCachedStubs(ObjectInput in) throws IOException, ClassNotFoundException {
        return (T) readObject(in);
    }

    private static Class<?> getClass(int classCode) {
        Class<?> clazz = _codeToClass.get(classCode);
        if (clazz != null)
            return clazz;

        synchronized (_classToCode) {
            return _codeToClass.get(classCode);
        }
    }

    private static int getCode(Class<?> clazz) {
        if (_classToCode.containsKey(clazz))
            return _classToCode.get(clazz);

        synchronized (_classToCode) {
            if (_classToCode.containsKey(clazz))
                return _classToCode.get(clazz);

            int associatedKey = _swapExtenKey++;
            _classToCode.put(clazz, associatedKey);
            _codeToClass.put(associatedKey, clazz);
            return associatedKey;
        }
    }
    /**
     * Tasks are loaded with a fresh class loader. When the task is done this fresh class loader is
     * removed. This will make it possible to load a modified version of this class GS-12351-
     * Running Distributed Task can throw ClassNotFoundException, if this task was loaded by a
     * client that already shutdown aInternalSpaceTaskWrapper.java:155nd the class has more dependencies to load. GS-12352 -
     * Distributed Task class is not unloaded after the task finish. GS-12295 - Distributed task -
     * improve class loading mechanism.
     *
     * @see com.gigaspaces.internal.server.space.SpaceImpl#executeTask(SpaceTask, Transaction,
     * SpaceContext, boolean)
     */
    public static Object readObject(ObjectInput in, SupportCodeChangeAnnotationContainer supportCodeChangeAnnotationContainer, boolean useIOUtilsReadObject) throws ClassNotFoundException, IOException {
        if(supportCodeChangeAnnotationContainer == null){
            return readObject(in, useIOUtilsReadObject);
        }
        ClassLoader current = ClassLoaderHelper.getContextClassLoader();
        try {
            ClassLoader codeChangeClassLoader = null;
            if(current instanceof ServiceClassLoader){
                codeChangeClassLoader = ((ServiceClassLoader) current).getCodeChangeClassLoader(supportCodeChangeAnnotationContainer);
            }
            else { // "pure" spaceTask
                codeChangeClassLoader = CodeChangeClassLoadersManager.getInstance().getCodeChangeClassLoader(supportCodeChangeAnnotationContainer);
            }
            ClassLoaderHelper.setContextClassLoader(codeChangeClassLoader, true);
            return readObject(in, useIOUtilsReadObject);
        } finally {
            ClassLoaderHelper.setContextClassLoader(current, true);

        }
    }

    private static Object readObject(ObjectInput in, boolean useIOUtilsReadObject) throws IOException, ClassNotFoundException {
        if(useIOUtilsReadObject){
            return IOUtils.readObject(in);
        }
        else {
            return in.readObject();
        }
    }

    /**
     * Complement of {@link #deserializeSupportCodeChangeCollection(ObjectInput in, Collection collection))}
     */
    public static void serializeSupportCodeChangeCollection(ObjectOutput out, Collection collection) throws IOException {
        out.writeInt(collection.size());
        for (Object object : collection) {
            SupportCodeChangeAnnotationContainer supportCodeChangeAnnotationContainer = CodeChangeUtilities.createContainerFromSupportCodeAnnotationIfNeeded(object);
            out.writeObject(supportCodeChangeAnnotationContainer);
            out.writeObject(object);
        }
    }

    /**
     * Complement of {@link #serializeSupportCodeChangeCollection(ObjectOutput out, Collection objects)}
     */
    public static void deserializeSupportCodeChangeCollection(ObjectInput in, Collection collection) throws IOException, ClassNotFoundException {
        int collectionSize = in.readInt();
        for (int i = 0; i < collectionSize; i++) {
            SupportCodeChangeAnnotationContainer codeChangeAnnotationContainer = (SupportCodeChangeAnnotationContainer) in.readObject();
            Object object = IOUtils.readObject(in, codeChangeAnnotationContainer, false);
            //noinspection unchecked
            collection.add(object);
        }
    }

    public static void writeShort(ObjectOutput out, short value) throws IOException {
        writeInt(out, value);
    }

    public static void writeInt(ObjectOutput out, int value) throws IOException {
        byte b = 0;
        if (value < 0) {
            b = 64;
            value = ~value;
        }
        b |= (byte) (value & 0x3f);

        for (value = (int) (value >> 6); value != 0; value = (int) (value >> 7)) {
            b |= 0x80;
            out.writeByte(b);
            b = (byte) (value & 0x7f);
        }
        out.writeByte(b);
    }

    public static void writeLong(ObjectOutput out, long value) throws IOException {
        byte b = 0;
        if (value < 0L) {
            b = 64;
            value = ~value;
        }
        b |= (byte) ((int) value & 0x3f);
        for (value = (long) value >> 6; value != 0L; value = (long) (value) >> 7) {
            b |= 0x80;
            out.writeByte(b);
            b = (byte) ((int) value & 0x7f);
        }
        out.writeByte(b);
    }

    public static short readShort(ObjectInput in) throws IOException {
        return (short) readInt(in);
    }

    public static int readInt(ObjectInput in) throws IOException {
        int b = in.readByte();
        int value = b & 0x3f;
        int cBits = 6;
        boolean fNeg = (b & 0x40) != 0;
        while ((b & 0x80) != 0) {
            b = in.readByte();
            value |= (b & 0x7f) << cBits;
            cBits += 7;
        }
        if (fNeg)
            value = ~value;
        return value;
    }

    public static long readLong(ObjectInput in) throws IOException {
        int b = in.readByte();
        long l = b & 0x3f;
        int cBits = 6;
        boolean fNeg = (b & 0x40) != 0;
        while ((b & 0x80) != 0) {
            b = in.readByte();
            l |= (long) (b & 0x7f) << cBits;
            cBits += 7;
        }
        if (fNeg)
            l = ~l;
        return l;
    }

    public static Map<Class<?>, IClassSerializer<?>> getClassSerializers() {
        return _typeCache;
    }

    public static IClassSerializer<?> getDefaultSerializer() {
        return _defaultSerializer;
    }
}