package com.gigaspaces.internal.jvm;

import com.gigaspaces.api.InternalApi;
import com.gigaspaces.internal.utils.concurrent.UnsafeHolder;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Based on Java memory spec, and specifically https://github.com/jbellis/jamm/blob/master/src/org/github/jamm/MemoryLayoutSpecification.java
 * @author Niv Ingberg
 * @since 15.5
 */
@InternalApi
public class HeapUsageEstimator {

    private final String desc;
    private final int arrayHeaderSize;
    private final int objectHeaderSize;
    private final int objectPadding;
    private final int referenceSize;
    private final int superclassFieldPadding;

    public HeapUsageEstimator() {
        this(Builder.auto());
    }

    private HeapUsageEstimator(Builder builder) {
        this.desc = builder.desc;
        this.arrayHeaderSize = builder.arrayHeaderSize;
        this.objectHeaderSize = builder.objectHeaderSize;
        this.objectPadding = builder.objectPadding;
        this.referenceSize = builder.referenceSize;
        this.superclassFieldPadding = builder.superclassFieldPadding;
    }

    public String getDesc() {
        return desc;
    }

    public int getObjectHeaderSize() {
        return objectHeaderSize;
    }

    public int getObjectPadding() {
        return objectPadding;
    }

    public int getSuperclassFieldPadding() {
        return superclassFieldPadding;
    }

    public int getReferenceSize() {
        return referenceSize;
    }

    /**
     * @return The size of the provided instance as defined by the detected MemoryLayoutSpecification. For an array this
     * is dependent on the size of the array, but for an object this is fixed for all instances
     */
    public long sizeOf(Object obj) {
        Class<?> type = obj.getClass();
        return type.isArray() ? sizeOfArray(type.getComponentType(), Array.getLength(obj)) : sizeOfInstance(type);
    }

    /**
     * @return The memory size of a field of a class of the provided type; for Objects this is the size of the reference only
     */
    public int sizeOfField(Class<?> type) {
        if (!type.isPrimitive())
            return referenceSize;
        if (type == boolean.class || type == byte.class)
            return 1;
        else if (type == char.class || type == short.class)
            return 2;
        else if (type == float.class || type == int.class)
            return 4;
        else if (type == double.class || type == long.class)
            return 8;
        throw new IllegalArgumentException("Unsupported type: " + type);
    }

    private int sizeOfFields(Iterable<Field> fields) {
        int size = 0;
        for (Field f : fields)
            size += sizeOfField(f.getType());
        return size;
    }

    public long sizeOfInstance(Class<?> type) {
        if (!UnsafeHolder.isAvailable()) {
            // this is very close to accurate, but occasionally yields a slightly incorrect answer (when long fields are used
            // and cannot be 8-byte aligned, an extra 4-bytes is allocated.
            long size = objectHeaderSize + sizeOfFields(declaredFieldsOf(type));
            while ((type = type.getSuperclass()) != Object.class && type != null)
                size += roundTo(sizeOfFields(declaredFieldsOf(type)), superclassFieldPadding);
            return roundTo(size, objectPadding);
        }
        // attempts to use sun.misc.Unsafe to find the maximum object offset, this work around helps deal with long alignment
        while (type != null) {
            long size = 0;
            for (Field f : declaredFieldsOf(type))
                size = Math.max(size, UnsafeHolder.objectFieldOffset(f) + sizeOfField(f.getType()));
            if (size > 0)
                return roundTo(size, objectPadding);
            type = type.getSuperclass();
        }
        return roundTo(objectHeaderSize, objectPadding);
    }

    public long sizeOfArray(Class<?> type, int length) {
        return roundTo(arrayHeaderSize + length * sizeOfField(type), objectPadding);
    }

    private static Collection<Field> declaredFieldsOf(Class<?> type) {
        List<Field> fields = new ArrayList<Field>();
        for (Field f : type.getDeclaredFields())
        {
            if (!Modifier.isStatic(f.getModifiers()))
                fields.add(f);
        }
        return fields;
    }

    public static long roundTo(long x, int multiple) {
        return ((x + multiple - 1) / multiple) * multiple;
    }

    public static class Builder {

        private String desc;
        private int arrayHeaderSize;
        private int objectHeaderSize;
        private int objectPadding;
        private int referenceSize;
        private int superclassFieldPadding;

        public HeapUsageEstimator build() {
            return new HeapUsageEstimator(this);
        }

        public Builder desc(String desc) {
            this.desc = desc;
            return this;
        }

        public Builder arrayHeaderSize(int arrayHeaderSize) {
            this.arrayHeaderSize = arrayHeaderSize;
            return this;
        }

        public Builder objectHeaderSize(int objectHeaderSize) {
            this.objectHeaderSize = objectHeaderSize;
            return this;
        }

        public Builder objectPadding(int objectPadding) {
            this.objectPadding = objectPadding;
            return this;
        }

        public Builder referenceSize(int referenceSize) {
            this.referenceSize = referenceSize;
            return this;
        }

        public Builder superclassFieldPadding(int superclassFieldPadding) {
            this.superclassFieldPadding = superclassFieldPadding;
            return this;
        }

        public static Builder auto() {
            final String dataModel = System.getProperty("sun.arch.data.model");
            if ("32".equals(dataModel)) {
                return auto32bit();
            } if (JavaUtils.useCompressedOopsAsBoolean()) {
                return auto64bitCompressedOops();
            } else { // Default:  64-bit uncompressed OOPs object model
                return auto64bit();
            }
        }

        public static Builder auto32bit() {
            return new Builder()
                    .desc("32bit")
                    .arrayHeaderSize(12)
                    .objectHeaderSize(8)
                    .objectPadding(8)
                    .referenceSize(4)
                    .superclassFieldPadding(4);
        }

        public static Builder auto64bit() {
            return new Builder()
                    .desc("64bit")
                    .arrayHeaderSize(24)
                    .objectHeaderSize(16)
                    .objectPadding(getObjectAlignmentInBytes())
                    .referenceSize(8)
                    .superclassFieldPadding(8);
        }

        public static Builder auto64bitCompressedOops() {
            return new Builder()
                    .desc("64bit-CompressedOops")
                    .arrayHeaderSize(16)
                    .objectHeaderSize(12)
                    .objectPadding(getObjectAlignmentInBytes())
                    .referenceSize(4)
                    .superclassFieldPadding(4);
        }

        private static int getObjectAlignmentInBytes() {
            RuntimeMXBean runtimeMxBean = ManagementFactory.getRuntimeMXBean();
            for (String arg : runtimeMxBean.getInputArguments()) {
                if (arg.startsWith("-XX:ObjectAlignmentInBytes=")) {
                    try {
                        return Integer.parseInt(arg.substring("-XX:ObjectAlignmentInBytes=".length()));
                    } catch (Exception e){}
                }
            }
            return 8;
        }
    }

    public static void main(String[] args) {
        HeapUsageEstimator calc = Builder.auto64bit().build();
        System.out.println(calc.sizeOfInstance(Object.class));
        System.out.println(calc.sizeOfInstance(Integer.class));
        System.out.println(calc.sizeOfInstance(Long.class));
        System.out.println(calc.sizeOfInstance(String.class));
    }
}
