package com.gigaspaces.internal.utils;

import java.util.Arrays;
import java.util.Objects;

public class CompoundIdUtils {
    public static String getCompoundIdToString(Object... objects){
        boolean allNull = true;
        for (Object object : objects) {
            if (object != null) {
                allNull = false;
                break;
            }
        }
        return allNull ? null : Arrays.toString(objects);
    }
//boolean allNull = Arrays.stream(objects).noneMatch(Objects::nonNull);

    public static int getCompoundIdHashCode(Object... objects) {
        return Objects.hash(objects);
    }

}
