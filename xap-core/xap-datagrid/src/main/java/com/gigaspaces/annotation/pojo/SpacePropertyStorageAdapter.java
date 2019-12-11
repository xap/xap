package com.gigaspaces.annotation.pojo;

import com.gigaspaces.api.ExperimentalApi;
import com.gigaspaces.client.storage_adapters.PropertyStorageAdapter;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

@Target(METHOD)
@Retention(RUNTIME)
@ExperimentalApi
public @interface SpacePropertyStorageAdapter {
    Class<? extends PropertyStorageAdapter> value();
}
