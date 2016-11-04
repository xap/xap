package com.gigaspaces.query.extension.metadata;

import java.io.Serializable;

/**
 * @author Vitaliy_Zinchenko
 */
public abstract class QueryExtensionActionInfo implements Serializable {

    private static final long serialVersionUID = 1L;

    public abstract boolean isIndexed();

}
