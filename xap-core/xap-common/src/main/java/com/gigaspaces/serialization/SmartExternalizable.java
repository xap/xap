package com.gigaspaces.serialization;

import com.gigaspaces.api.ExperimentalApi;

import java.io.Externalizable;

/**
 * Externalizable markup extension for GigaSpaces serialization - when an instance
 * of this interface is serialized, GigaSpaces generates a factory class to instantiate
 * it using the default constructor, thereby speeding up serialization significantly.
 * The relevant GigaSpaces serializable classes extend this interface.
 *
 * @author Niv Ingberg
 * @since 16.0
 */
@ExperimentalApi
public interface SmartExternalizable extends Externalizable {
}
