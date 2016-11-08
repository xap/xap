package com.gigaspaces.query.extension.metadata.impl;

import com.gigaspaces.query.extension.metadata.QueryExtensionAnnotationAttributesInfo;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Default implementation of the {@link QueryExtensionAnnotationAttributesInfo}. Default implementation is
 * indexed and doesnt have additiona attributes.
 *
 * @author Vitaliy_Zinchenko
 * @since 12.1
 *
 * @see QueryExtensionAnnotationAttributesInfo
 */
public class DefaultQueryExtensionPathAnnotationAttributesInfo implements QueryExtensionAnnotationAttributesInfo, Externalizable {

    private static final long serialVersionUID = 1L;

    public DefaultQueryExtensionPathAnnotationAttributesInfo() {
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {

    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {

    }

    @Override
    public boolean isIndexed() {
        return true;
    }
}
