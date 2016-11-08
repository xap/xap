package org.openspaces.textsearch;

import com.gigaspaces.query.extension.metadata.QueryExtensionAnnotationAttributesInfo;

import java.io.Serializable;

/**
 * @author Vitaliy_Zinchenko
 */
public class TextAnalyzerQueryExtensionAnnotationAttributesInfo implements QueryExtensionAnnotationAttributesInfo {
    private static final long serialVersionUID = 1L;

    private Class clazz;

    public TextAnalyzerQueryExtensionAnnotationAttributesInfo() {
    }

    public TextAnalyzerQueryExtensionAnnotationAttributesInfo(Class clazz) {
        this.clazz = clazz;
    }

    public Class getAnalazerClass() {
        return clazz;
    }

    public TextAnalyzerQueryExtensionAnnotationAttributesInfo setClazz(Class clazz) {
        this.clazz = clazz;
        return this;
    }

    @Override
    public boolean isIndexed() {
        return false;
    }
}
