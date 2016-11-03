package com.gigaspaces.query.extension.metadata;

import java.lang.annotation.Annotation;

/**
 * @author Vitaliy_Zinchenko
 */
public class DefaultQueryExtensionInfo implements QueryExtensionInfo {

    private Class<? extends Annotation> queryExtensionAnnotation;
    private QueryExtensionPathActionInfo queryExtensionPathActionInfo;

    public DefaultQueryExtensionInfo(Class<? extends Annotation> queryExtensionAnnotation, QueryExtensionPathActionInfo queryExtensionPathActionInfo) {
        this.queryExtensionAnnotation = queryExtensionAnnotation;
        this.queryExtensionPathActionInfo = queryExtensionPathActionInfo;
    }

    @Override
    public Class<? extends Annotation> getQueryExtensionAnnotation() {
        return queryExtensionAnnotation;
    }

    @Override
    public QueryExtensionPathActionInfo getQueryExtensionPathActionInfo() {
        return queryExtensionPathActionInfo;
    }
}
