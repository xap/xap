package com.gigaspaces.query.extension.metadata.impl;

import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.query.extension.metadata.QueryExtensionAnnotationAttributesInfo;
import com.gigaspaces.query.extension.metadata.QueryExtensionAnnotationInfo;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.annotation.Annotation;

/**
 * @author Vitaliy_Zinchenko
 * @since 12.1
 */
public class QueryExtensionAnnotationInfoImpl implements QueryExtensionAnnotationInfo, Externalizable {

    private Class<? extends Annotation> type;
    private QueryExtensionAnnotationAttributesInfo attributes;

    public QueryExtensionAnnotationInfoImpl() {
    }

    public QueryExtensionAnnotationInfoImpl(Class<? extends Annotation> type, QueryExtensionAnnotationAttributesInfo attributes) {
        this.type = type;
        this.attributes = attributes;
    }

    public Class<? extends Annotation> getType() {
        return type;
    }

    public QueryExtensionAnnotationAttributesInfo getAttributes() {
        return attributes;
    }

    @Override
    public boolean isIndexed() {
        return attributes.isIndexed();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        IOUtils.writeObject(out, type);
        IOUtils.writeObject(out, attributes);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        type = IOUtils.readObject(in);
        attributes = IOUtils.readObject(in);
    }
}
