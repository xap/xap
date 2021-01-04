package com.gigaspaces.metadata;

import com.gigaspaces.client.storage_adapters.class_storage_adapters.ClassBinaryStorageAdapter;
import com.gigaspaces.client.storage_adapters.class_storage_adapters.DirectClassBinaryStorageAdapter;
import com.gigaspaces.client.storage_adapters.class_storage_adapters.SequentialClassBinaryStorageAdapter;
import com.gigaspaces.internal.utils.GsEnv;
import com.j_spaces.kernel.ClassLoaderHelper;
import com.j_spaces.kernel.SystemProperties;

/**
 * @author Niv Ingberg
 * @since 15.8
 */
public enum ClassBinaryStorageLayout {
    DEFAULT,
    SEQUENTIAL,
    DIRECT;

    private final Class<? extends ClassBinaryStorageAdapter> adapterClass;

    ClassBinaryStorageLayout() {
        this.adapterClass = initAdapter(this.name());
    }

    private static Class<? extends ClassBinaryStorageAdapter> initAdapter(String name) {
        String value = GsEnv.property(SystemProperties.CLASS_BINARY_STORAGE_ADAPTER + name.toLowerCase()).get();
        if (value != null) {
            try {
                return ClassLoaderHelper.loadClass(value);
            } catch (ClassNotFoundException e) {
                throw new SpaceMetadataException("Failed to load class [" + value + "]");
            }
        }
        return name.equals("DIRECT") ? DirectClassBinaryStorageAdapter.class : SequentialClassBinaryStorageAdapter.class;
    }

    public Class<? extends ClassBinaryStorageAdapter> getAdapterClass() {
        return adapterClass;
    }
}
