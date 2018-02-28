package org.openspaces.core.config;

/**
 * @author Yael Nahon
 * @since 12.3
 */
public class EqualIndex extends SpaceIndex{

    public EqualIndex() {
        super();
    }

    public EqualIndex(String indexPropertyPath) {
        super(indexPropertyPath);
    }

    public EqualIndex(boolean unique) {
        super(unique);
    }

    public EqualIndex(String indexPropertyPath, boolean unique) {
        super(indexPropertyPath, unique);
    }

}
