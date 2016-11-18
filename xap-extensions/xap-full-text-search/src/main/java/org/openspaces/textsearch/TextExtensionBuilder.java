package org.openspaces.textsearch;

import com.gigaspaces.query.extension.metadata.QueryExtensionAnnotationInfo;
import com.gigaspaces.query.extension.metadata.impl.DefaultQueryExtensionAnnotationInfo;

/**
 * @author Vitaliy_Zinchenko
 */
public class TextExtensionBuilder {

    public static QueryExtensionAnnotationInfo analyzer(Class analyzerClass) {
        return new TextAnalyzerQueryExtensionAnnotationInfo(SpaceTextAnalyzer.class, analyzerClass);
    }

    public static QueryExtensionAnnotationInfo index() {
        return new DefaultQueryExtensionAnnotationInfo(SpaceTextIndex.class);
    }

}
