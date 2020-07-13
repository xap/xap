package com.gigaspaces.client.iterator.internal;

import com.gigaspaces.api.InternalApi;

import java.util.Iterator;

@InternalApi
public class ArrayIterator<T> implements Iterator<T> {
    private final Object[] array;
    private int pos;

    public static <T> ArrayIterator<T> wrap(Object[] array) {
        if (array == null)
            return null;
        return new ArrayIterator<>(array);
    }

    private ArrayIterator(Object[] array) {
        this.array = array;
    }

    @Override
    public boolean hasNext() {
        return pos < array.length;
    }

    @Override
    public T next() {
        return (T)array[pos++];
    }
}
