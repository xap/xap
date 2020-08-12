package com.j_spaces.core.cluster;

import java.util.Arrays;

/**
 * @author Yael Nahon
 * @since 12.3
 */
public enum RedoLogCompaction {
    NONE("none"),
    MIRROR("mirror");
    private final String name;

    RedoLogCompaction(String name) {
        this.name = name;
    }

    public static RedoLogCompaction parse(String val){
        if(val.equalsIgnoreCase(NONE.name)) return NONE;
        if(val.equalsIgnoreCase(MIRROR.name)) return MIRROR;
        throw new IllegalArgumentException("illegal redo log compaction target, must be one of: " + Arrays.toString(RedoLogCompaction.values()));
    }
}
