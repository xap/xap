package org.openspaces.textsearch;

import com.gigaspaces.metadata.SpaceTypeDescriptor;
import com.gigaspaces.query.extension.metadata.QueryExtensionPathInfo;
import com.gigaspaces.query.extension.metadata.TypeQueryExtension;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
import org.openspaces.spatial.lucene.common.BaseLuceneConfiguration;
import org.openspaces.spatial.lucene.common.BaseLuceneTypeIndex;
import org.openspaces.spatial.lucene.common.Utils;

import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.io.IOException;
import java.lang.annotation.Annotation;
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

    //TODO refactor/fix for document support
    private Analyzer getMainAnalyzer(BaseLuceneConfiguration luceneConfig, SpaceTypeDescriptor typeDescriptor) {
        if (typeDescriptor.getObjectClass() != null && typeDescriptor.getObjectClass().isAnnotationPresent(SpaceTextAnalyzer.class)) {
            Class analyzerClass = typeDescriptor.getObjectClass().getAnnotation(SpaceTextAnalyzer.class).clazz();
            return Utils.createAnalyzer(analyzerClass);
        } else {
            return luceneConfig.getDefaultAnalyzer();
        }
    }

    //TODO refactor/fix for document support
    private Map<String, Analyzer> createFieldAnalyzers(SpaceTypeDescriptor typeDescriptor) {
        Map<String, Analyzer> analyzerMap = new HashMap<String, Analyzer>();
        TypeQueryExtension type = typeDescriptor.getQueryExtensions().getByNamespace(LuceneTextSearchQueryExtensionProvider.NAMESPACE);
        for (String path : type.getPaths()) {
            QueryExtensionPathInfo pathInfo = type.get(path);
            for (Class<? extends Annotation> action : pathInfo.getActions()) {
                if (SpaceTextAnalyzer.class.equals(action)) {
                    TextAnalyzerQueryExtensionPathActionInfo analyzerActionInfo = (TextAnalyzerQueryExtensionPathActionInfo) pathInfo.getActionInfo(action);
                    addAnalyzer(analyzerMap, path, analyzerActionInfo.getAnalazerClass());
                }
            }
        }
        return analyzerMap;
    }

    private void addAnalyzer(Map<String, Analyzer> analyzerMap, String path, Class clazz) {
        Analyzer analyzer = Utils.createAnalyzer(clazz);
        analyzerMap.put(path, analyzer);
    }
}
