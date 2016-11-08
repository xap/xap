package com.gigaspaces.query.extension.metadata.typebuilder;

import com.gigaspaces.query.extension.metadata.QueryExtensionAnnotationAttributesInfo;

import java.lang.annotation.Annotation;

/**
 * @author Vitaliy_Zinchenko
 * @since 12.1
 */
public class DefaultQueryExtensionInfo implements QueryExtensionInfo {

    private Class<? extends Annotation> queryExtensionAnnotation;
    private QueryExtensionAnnotationAttributesInfo queryExtensionPathActionInfo;

    public DefaultQueryExtensionInfo(Class<? extends Annotation> queryExtensionAnnotation, QueryExtensionAnnotationAttributesInfo queryExtensionPathActionInfo) {
        this.queryExtensionAnnotation = queryExtensionAnnotation;
        this.queryExtensionPathActionInfo = queryExtensionPathActionInfo;
    }

    @Override
    public Class<? extends Annotation> getQueryExtensionAnnotation() {
        return queryExtensionAnnotation;
    }

    @Override
    public QueryExtensionAnnotationAttributesInfo getQueryExtensionActionInfo() {
        return queryExtensionPathActionInfo;
    }
}
