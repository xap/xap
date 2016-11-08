package com.gigaspaces.query.extension.metadata;

import java.io.Serializable;

/**
 * Abstraction encapsulates query extension annotation's attributes.
 *
 * @author Vitaliy_Zinchenko
 * @since 12.1
 *
 */
public interface QueryExtensionAnnotationAttributesInfo extends Serializable {

    boolean isIndexed();

}
