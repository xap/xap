package com.gigaspaces.internal.utils;

import java.util.Arrays;
import java.util.Objects;

public class CompoundIdUtils {
    public static String getCompoundIdToString(Object... objects){
        return Arrays.toString(objects);
    }

    public static int getCompoundIdHashCode(Object... objects) {
        return Objects.hash(objects);
    }

    public static boolean getCompoundIdEquals(Object[] otherObjects,Object[] myObjects){
        return Arrays.equals(otherObjects,myObjects);
    }
}
