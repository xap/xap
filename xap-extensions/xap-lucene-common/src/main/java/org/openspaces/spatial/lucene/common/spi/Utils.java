package org.openspaces.spatial.lucene.common.spi;

import org.apache.lucene.analysis.Analyzer;

/**
 * @author Vitaliy_Zinchenko
 */
final public class Utils {

    public Utils() {
    }

    public static Analyzer createAnalyzer(Class analyzerClass) {
        try {
            return (Analyzer) analyzerClass.newInstance();
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to instantiate analyzer " + analyzerClass);
        }
    }

}
