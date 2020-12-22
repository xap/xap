package com.gigaspaces.start;

/**
 * @author Niv Ingberg
 * @since 12.2
 */
public enum ProductType {
    XAP, InsightEdge;

    private final String nameLowerCase = this.name().toLowerCase();

    public String getNameLowerCase() {
        return nameLowerCase;
    }
}
