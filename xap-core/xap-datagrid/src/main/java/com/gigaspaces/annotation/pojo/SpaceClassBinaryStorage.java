package com.gigaspaces.annotation.pojo;

import com.gigaspaces.api.ExperimentalApi;
import com.gigaspaces.client.storage_adapters.class_storage_adapters.ClassBinaryStorageAdapter;
import com.gigaspaces.client.storage_adapters.class_storage_adapters.DefaultClassBinaryStorageAdapter;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Indicates that entries of this class will be stored in-memory in a packed binary format. Binary storage reduces
 * memory consumption, but requires unpacking when accessing the data at the server side (e.g. matching or aggregation).
 *
 * By default, non-indexed properties are stored in binary format, whereas indexed properties are stored in object format,
 * providing a balanced tradeoff between performance and memory footprint. You can override that default per property
 * using the {@link SpacePropertyStorage} annotation.
 *
 * @see SpacePropertyStorage
 * @author Yael Nahon, Niv Ingberg
 * @since 15.8
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface SpaceClassBinaryStorage {
    /**
     * Sets the adapter to be used for packing/unpacking properties.
     * NOTE: this setting is experimental and subject to breaking changes in future versions.
     */
    @ExperimentalApi
    Class<? extends ClassBinaryStorageAdapter> adapter() default DefaultClassBinaryStorageAdapter.class;
}
