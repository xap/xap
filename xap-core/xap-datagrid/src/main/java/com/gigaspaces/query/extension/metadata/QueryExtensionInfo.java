package com.gigaspaces.query.extension.metadata;

import java.lang.annotation.Annotation;

/**
 * @author Vitaliy_Zinchenko
 */
public interface QueryExtensionInfo {

    Class<? extends Annotation> getQueryExtensionAnnotation();

    QueryExtensionActionInfo getQueryExtensionActionInfo();

}
