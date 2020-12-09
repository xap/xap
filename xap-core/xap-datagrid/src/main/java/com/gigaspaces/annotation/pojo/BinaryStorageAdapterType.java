package com.gigaspaces.annotation.pojo;

public enum BinaryStorageAdapterType {
    EXCLUDE_INDEXES, CUSTOM;

    public static BinaryStorageAdapterType fromCode(int code) {
        switch (code) {
            case 1:
                return EXCLUDE_INDEXES;
            case 2:
                return CUSTOM;
            default:
                throw new IllegalStateException("No StorageAdapterType found for code: " + code);
        }
    }

    public static int toCode(BinaryStorageAdapterType type) {
        switch (type) {
            case EXCLUDE_INDEXES:
                return 1;
            case CUSTOM:
                return 2;
            default:
                throw new IllegalStateException("No StorageAdapterType found [" + type + "]");
        }
    }
}
