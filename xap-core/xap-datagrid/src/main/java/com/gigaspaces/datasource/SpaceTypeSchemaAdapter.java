package com.gigaspaces.datasource;

import com.gigaspaces.document.SpaceDocument;
import com.gigaspaces.metadata.SpaceTypeDescriptor;

import java.io.Serializable;

/**
 * @author Alon Shoham
 * @since 15.5.0
 */
public interface SpaceTypeSchemaAdapter extends Serializable {
    SpaceDocument adaptEntry(SpaceDocument spaceDocument);

    SpaceTypeDescriptor adaptTypeDescriptor(SpaceTypeDescriptor spaceTypeDescriptor);

    String getTypeName();
}
