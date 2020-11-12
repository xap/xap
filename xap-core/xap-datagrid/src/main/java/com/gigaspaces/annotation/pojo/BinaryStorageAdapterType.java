package com.gigaspaces.annotation.pojo;

public enum BinaryStorageAdapterType {
    ALL, EXCLUDE_INDEXES, CUSTOM;

    public static BinaryStorageAdapterType fromCode(int code) {
        switch (code) {
            case 1:
                return ALL;
            case 2:
                return EXCLUDE_INDEXES;
            case 3:
                return CUSTOM;
            default:
                throw new IllegalStateException("No StorageAdapterType found for code: " + code);
        }
    }

    public static int toCode(BinaryStorageAdapterType type) {
        switch (type) {
            case ALL:
                return 1;
            case EXCLUDE_INDEXES:
                return 2;
            case CUSTOM:
                return 3;
            default:
                throw new IllegalStateException("No StorageAdapterType found [" + type + "]");
        }
    }
}
