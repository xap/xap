package org.openspaces.textsearch;

import com.gigaspaces.query.extension.metadata.DefaultQueryExtensionInfo;
import com.gigaspaces.query.extension.metadata.QueryExtensionInfo;

/**
 * @author Vitaliy_Zinchenko
 */
public class TextQueryExtensionInfoBuilder {

    public static AnalyzerBuilder analyzer() {
        return new AnalyzerBuilder();
    }

    public static class AnalyzerBuilder {
        private Class clazz;

        public AnalyzerBuilder setClass(Class clazz) {
            this.clazz = clazz;
            return this;
        }

        public QueryExtensionInfo build() {
            TextAnalyzerQueryExtensionPathActionInfo actionInfo = new TextAnalyzerQueryExtensionPathActionInfo()
                    .setClazz(clazz);
            return new DefaultQueryExtensionInfo(SpaceTextAnalyzer.class, actionInfo);
        }
    }

}
