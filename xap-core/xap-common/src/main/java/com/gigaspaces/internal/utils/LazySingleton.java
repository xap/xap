package com.gigaspaces.internal.utils;

import java.util.function.Supplier;

/**
 * @author Niv Ingberg
 * @since 15.5
 */
public class LazySingleton<T> {
    private final Supplier<T> factory;
    private volatile T ref;

    public LazySingleton(Supplier<T> factory) {
        this.factory = factory;
    }

    public T getOrCreate() {
        // Fast track - single volatile read, return result if not null
        T snapshot = ref;
        if (snapshot != null)
            return snapshot;
        // If not initialized yet, sync to continue
        synchronized (this) {
            // If still not initialized, init
            if (ref == null)
                ref = factory.get();
            return ref;
        }
    }
}
