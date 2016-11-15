package org.openspaces.textsearch;

import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.query.extension.metadata.QueryExtensionAnnotationAttributesInfo;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * @author Vitaliy_Zinchenko
 */
public class TextAnalyzerQueryExtensionAnnotationAttributesInfo implements QueryExtensionAnnotationAttributesInfo, Externalizable {

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

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        IOUtils.writeObject(out, clazz);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        clazz = IOUtils.readObject(in);
    }
}
