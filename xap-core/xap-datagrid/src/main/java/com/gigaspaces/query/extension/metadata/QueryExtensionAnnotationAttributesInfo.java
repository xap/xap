package com.gigaspaces.query.extension.metadata;

/**
 * Represents query extension annotation's attributes.
 *
 * @author Vitaliy_Zinchenko
 * @since 12.1
 *
 */
public interface QueryExtensionAnnotationAttributesInfo {

    /**
     * Determines is this annotation indexed or not
     * @return true if annotation indexed, false otherwise
     */
    boolean isIndexed();

}
