package com.gigaspaces.query.extension.metadata.impl;

import com.gigaspaces.internal.io.IOUtils;
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
public class DefaultQueryExtensionAnnotationInfo implements QueryExtensionAnnotationInfo, Externalizable {

    private static final long serialVersionUID = 1L;

    private Class<? extends Annotation> type;

    public DefaultQueryExtensionAnnotationInfo() {
    }

    public DefaultQueryExtensionAnnotationInfo(Class<? extends Annotation> type) {
        this.type = type;
    }

    @Override
    public Class<? extends Annotation> getType() {
        return type;
    }

    @Override
    public boolean isIndexed() {
        return true;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        IOUtils.writeObject(out, type);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        type = IOUtils.readObject(in);
    }
}
