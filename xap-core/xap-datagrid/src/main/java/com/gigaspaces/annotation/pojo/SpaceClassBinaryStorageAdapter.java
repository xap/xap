package com.gigaspaces.annotation.pojo;

import com.gigaspaces.client.storage_adapters.class_storage_adapters.ClassBinaryStorageAdapter;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.RetentionPolicy.RUNTIME;

@Target(ElementType.TYPE)
@Retention(RUNTIME)
public @interface SpaceClassBinaryStorageAdapter {
    Class<? extends ClassBinaryStorageAdapter> value();
}
