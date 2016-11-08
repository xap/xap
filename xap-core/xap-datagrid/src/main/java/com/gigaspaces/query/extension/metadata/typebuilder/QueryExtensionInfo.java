package com.gigaspaces.query.extension.metadata.typebuilder;

import com.gigaspaces.query.extension.metadata.QueryExtensionAnnotationAttributesInfo;

import java.lang.annotation.Annotation;

/**
 * Abstraction associates action type with {@link QueryExtensionAnnotationAttributesInfo} action info.
 *
 * @author Vitaliy_Zinchenko
 * @since 12.1
 */
public interface QueryExtensionInfo {

    Class<? extends Annotation> getQueryExtensionAnnotation();

    QueryExtensionAnnotationAttributesInfo getQueryExtensionActionInfo();

}
