package com.gigaspaces.internal.utils;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class CompoundIdUtils {
    public static String getCompoundId(List<Object> list){
        String result = list.stream().filter(Objects::nonNull)
                .map(Object::toString)
                .collect(Collectors.joining("_"));

        return result.isEmpty() ? null : result;
    }
}
