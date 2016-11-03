package org.openspaces.textsearch;

import com.gigaspaces.query.extension.metadata.QueryExtensionActionInfo;

/**
 * @author Vitaliy_Zinchenko
 */
public class TextAnalyzerQueryExtensionPathActionInfo extends QueryExtensionActionInfo {
    private static final long serialVersionUID = 1L;

    private Class clazz;

    public TextAnalyzerQueryExtensionPathActionInfo() {
    }

    public TextAnalyzerQueryExtensionPathActionInfo(Class clazz) {
        this.clazz = clazz;
    }

    public Class getAnalazerClass() {
        return clazz;
    }

    public TextAnalyzerQueryExtensionPathActionInfo setClazz(Class clazz) {
        this.clazz = clazz;
        return this;
    }
}
