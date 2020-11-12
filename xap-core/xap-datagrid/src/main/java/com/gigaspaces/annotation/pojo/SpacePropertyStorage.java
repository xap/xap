package com.gigaspaces.annotation.pojo;

import com.gigaspaces.metadata.StorageType;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * @author Yael Nahon, Niv Ingberg
 * @since 15.8
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface SpacePropertyStorage {
    /**
     * Sets the annotated property's storage type
     */
    StorageType value();
}
