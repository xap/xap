package org.openspaces.fts.spi;

import com.gigaspaces.metadata.SpaceTypeDescriptor;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
import org.openspaces.fts.SpaceTextAnalyzer;
import org.openspaces.spatial.lucene.common.spi.BaseLuceneConfiguration;
import org.openspaces.spatial.lucene.common.spi.BaseLuceneTypeIndex;
import org.openspaces.spatial.lucene.common.spi.Utils;

import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Vitaliy_Zinchenko
 */
public class LuceneTextSearchTypeIndex extends BaseLuceneTypeIndex {
    public LuceneTextSearchTypeIndex(BaseLuceneConfiguration luceneConfig, String namespace, SpaceTypeDescriptor typeDescriptor) throws IOException {
        super(luceneConfig, namespace, typeDescriptor);
    }

    @Override
    protected Analyzer createAnalyzer(BaseLuceneConfiguration luceneConfig, SpaceTypeDescriptor typeDescriptor) {
        Analyzer mainAnalyzer = getMainAnalyzer(luceneConfig, typeDescriptor);
        Map<String, Analyzer> fieldAnalyzers = createFieldAnalyzers(typeDescriptor);
        return new PerFieldAnalyzerWrapper(mainAnalyzer, fieldAnalyzers);
    }

    private Analyzer getMainAnalyzer(BaseLuceneConfiguration luceneConfig, SpaceTypeDescriptor typeDescriptor) {
        Class analyzerClass = null;
        if (typeDescriptor.getObjectClass().isAnnotationPresent(SpaceTextAnalyzer.class)) {
            analyzerClass = typeDescriptor.getObjectClass().getAnnotation(SpaceTextAnalyzer.class).clazz();
        } else {
            analyzerClass = luceneConfig.getDefaultAnalyzerClass();
        }
        return Utils.createAnalyzer(analyzerClass);
    }

    private Map<String, Analyzer> createFieldAnalyzers(SpaceTypeDescriptor typeDescriptor) {
        try {
            Map<String, Analyzer> analyzerMap = new HashMap<String, Analyzer>();
            PropertyDescriptor[] propertyDescriptors = Introspector.getBeanInfo(typeDescriptor.getObjectClass(), Object.class).getPropertyDescriptors();
            for (PropertyDescriptor propertyDescriptor: propertyDescriptors) {
                Method readMethod = propertyDescriptor.getReadMethod();
                if(readMethod.isAnnotationPresent(SpaceTextAnalyzer.class)) {
                    Class analyzerClass = readMethod.getAnnotation(SpaceTextAnalyzer.class).clazz();
                    Analyzer analyzer = Utils.createAnalyzer(analyzerClass);
                    analyzerMap.put(propertyDescriptor.getName(), analyzer);
                }
            }
            return analyzerMap;
        } catch (IntrospectionException e) {
            throw new IllegalArgumentException("Failed to get bean info of passed type " + typeDescriptor.getTypeName());
        }
    }
}
