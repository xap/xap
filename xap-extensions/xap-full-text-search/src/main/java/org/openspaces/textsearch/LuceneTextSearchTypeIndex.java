package org.openspaces.textsearch;

import com.gigaspaces.metadata.SpaceTypeDescriptor;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
import org.openspaces.spatial.lucene.common.BaseLuceneConfiguration;
import org.openspaces.spatial.lucene.common.BaseLuceneTypeIndex;
import org.openspaces.spatial.lucene.common.Utils;

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

    private Analyzer _mainAnalyzer;
    private Map<String, Analyzer> _fieldAnalyzers;

    public LuceneTextSearchTypeIndex(BaseLuceneConfiguration luceneConfig, String namespace, SpaceTypeDescriptor typeDescriptor) throws IOException {
        super(luceneConfig, namespace, typeDescriptor);
    }

    public Analyzer getAnalyzerForPath(String path) {
        Analyzer analyzer = _mainAnalyzer;
        Analyzer fieldAnalyzer = _fieldAnalyzers.get(path);
        if (fieldAnalyzer != null) {
            analyzer = fieldAnalyzer;
        }
        return analyzer;
    }

    @Override
    protected Analyzer createAnalyzer(BaseLuceneConfiguration luceneConfig, SpaceTypeDescriptor typeDescriptor) {
        _mainAnalyzer = getMainAnalyzer(luceneConfig, typeDescriptor);
        _fieldAnalyzers = createFieldAnalyzers(typeDescriptor);
        return new PerFieldAnalyzerWrapper(_mainAnalyzer, _fieldAnalyzers);
    }

    private Analyzer getMainAnalyzer(BaseLuceneConfiguration luceneConfig, SpaceTypeDescriptor typeDescriptor) {
        if (typeDescriptor.getObjectClass().isAnnotationPresent(SpaceTextAnalyzer.class)) {
            Class analyzerClass = typeDescriptor.getObjectClass().getAnnotation(SpaceTextAnalyzer.class).clazz();
            return Utils.createAnalyzer(analyzerClass);
        } else {
            return luceneConfig.getDefaultAnalyzer();
        }
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
