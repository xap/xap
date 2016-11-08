package org.openspaces.textsearch;

import com.gigaspaces.query.extension.metadata.impl.DefaultQueryExtensionPathAnnotationAttributesInfo;
import com.gigaspaces.query.extension.metadata.typebuilder.DefaultQueryExtensionInfo;
import com.gigaspaces.query.extension.metadata.typebuilder.QueryExtensionInfo;

/**
 * @author Vitaliy_Zinchenko
 */
public class TextExtensionBuilder {

    public static QueryExtensionInfo analyzer(Class clazz) {
        TextAnalyzerQueryExtensionAnnotationAttributesInfo attributes = new TextAnalyzerQueryExtensionAnnotationAttributesInfo()
                .setClazz(clazz);
        return new DefaultQueryExtensionInfo(SpaceTextAnalyzer.class, attributes);
    }

    public static QueryExtensionInfo index() {
        return new DefaultQueryExtensionInfo(SpaceTextIndex.class, new DefaultQueryExtensionPathAnnotationAttributesInfo());
    }

}
