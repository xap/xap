package org.openspaces.textsearch;

import com.gigaspaces.query.extension.metadata.QueryExtensionActionInfo;

/**
 * @author Vitaliy_Zinchenko
 */
public class TextAnalyzerQueryExtensionActionInfo extends QueryExtensionActionInfo {
    private static final long serialVersionUID = 1L;

    private Class clazz;

    public TextAnalyzerQueryExtensionActionInfo() {
    }

    public TextAnalyzerQueryExtensionActionInfo(Class clazz) {
        this.clazz = clazz;
    }

    public Class getAnalazerClass() {
        return clazz;
    }

    public TextAnalyzerQueryExtensionActionInfo setClazz(Class clazz) {
        this.clazz = clazz;
        return this;
    }

    @Override
    public boolean isIndexed() {
        return false;
    }
}
