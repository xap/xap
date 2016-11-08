package com.gigaspaces.query.extension.metadata;

import java.io.Serializable;

/**
 * @author Vitaliy_Zinchenko
 * @since 12.1
 *
 * Abstraction encapsulates query extension annotation's attributes.
 */
public interface QueryExtensionAnnotationAttributesInfo {

    boolean isIndexed();

}
