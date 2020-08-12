package com.j_spaces.kernel.list;

import java.lang.reflect.Array;
import java.util.*;
import java.util.function.Consumer;

public class CircularNumerator<T> {
    private final Iterator<T> iterator;
    private volatile T current;

    public CircularNumerator(T[] entries, int start) {
        if(entries == null)
            throw new IllegalArgumentException("entries cannot be null");
        iterator = createIteratorFromRandomStartPoint(entries, start);
        next();
    }

    public T getCurrent() {
        return current;
    }

    public void next(){
        current = iterator.hasNext() ? iterator.next() : null;
    }

    private Iterator<T> createIteratorFromRandomStartPoint(T[] entries, int start){
        int length = entries.length;
        if(length == 0)
            return new ArrayList<T>().iterator();
        if(start == 0)
            return Arrays.asList(entries).iterator();
        List<T> result = new ArrayList<>();
        for (int i = 0; i < length; i++, start++) {
            if(start >= length)
                start = 0;
            result.add(entries[start]);
        }
        return result.iterator();
    }
}
