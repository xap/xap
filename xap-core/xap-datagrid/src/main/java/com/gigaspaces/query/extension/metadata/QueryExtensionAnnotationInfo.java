package com.gigaspaces.query.extension.metadata;

import java.lang.annotation.Annotation;

/**
 * Represents query extension annotation.
 *
 * @author Vitaliy_Zinchenko
 * @since 12.1
 *
 */
public interface QueryExtensionAnnotationInfo {

    /**
     * Determines is this annotation indexed or not
     * @return true if annotation indexed, false otherwise
     */
    boolean isIndexed();

    /**
     * @return annotation type
     */
    Class<? extends Annotation> getType();

}
