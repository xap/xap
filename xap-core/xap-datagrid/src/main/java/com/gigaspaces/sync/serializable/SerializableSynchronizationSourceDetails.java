package com.gigaspaces.sync.serializable;

import com.gigaspaces.sync.SynchronizationSourceDetails;

import java.io.Serializable;

public class SerializableSynchronizationSourceDetails implements SynchronizationSourceDetails, Serializable {
    private static final long serialVersionUID = 8571013274007308800L;
    private final String name;

    public SerializableSynchronizationSourceDetails(SynchronizationSourceDetails synchronizationSourceDetails) {
        name = synchronizationSourceDetails.getName();
    }

    @Override
    public String getName() {
        return name;
    }
}
